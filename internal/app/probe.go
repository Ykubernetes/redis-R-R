package app

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// probeState 收敛探针与主流程共享的全部可变状态。
// 结构体化消除 startProbe 长串同类型指针位置参数——顺序写错编译器无法发现，
// 会静默破坏故障/恢复时刻的测量语义。
type probeState struct {
	// mu 保护时间戳、reportDone 与去抖计数。去抖计数只由探针 goroutine 写、
	// 主循环/statusReporter 读，统一入 mu 消除「atomic 与 mutex 双真相源」——
	// 此前 recovered 与 recoverTime 分属两套同步机制，语义一致仅靠写入顺序巧合维持。
	mu                 sync.Mutex
	faultTime          time.Time // 用户手动标记的故障注入时刻 T0
	firstFailTime      time.Time // 自动检测确认的故障开始时刻
	firstFailCandidate time.Time // 连续失败候选起点（去抖窗口首帧）
	recoverTime        time.Time // 业务恢复判定时刻；撤销后清零，flap 场景下为最终稳定恢复时刻
	reportDone         bool      // 报告生成后置位，禁止 T0 标记 goroutine 再写入
	consecOK           int       // 连续探测成功计数（mu 保护）
	consecFail         int       // 连续探测失败计数（mu 保护）
	recovered          bool      // 是否已判定恢复（mu 保护，与 recoverTime 同源）

	// 快照与统计：故障确认时冻结的「故障前最后成功写入」是 RPO 计算的上界。
	// writer 恢复后会继续写更大的 seq，若以运行结束的 lastSuccessSeq 为口径，
	// 其与扫描所得 maxSeq 恒等，丢失量恒为 0——RPO 指标完全失效。
	seqAtFault         atomic.Int64
	tsAtFault          atomic.Int64
	invalidations      atomic.Int64 // 恢复判定被撤销的次数
	totalProbeAttempts atomic.Int64
	successProbeCount  atomic.Int64
	probeFailCount     atomic.Int64
}

// isRecovered 恢复状态的唯一读取入口（锁内快照）
func (s *probeState) isRecovered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recovered
}

// counters 去抖计数与恢复状态的一致性快照，供 statusReporter 打印
func (s *probeState) counters() (cFail, cOK int, recovered bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consecFail, s.consecOK, s.recovered
}

// startProbe 周期性写入探针 key，自动检测故障开始与恢复时刻
func startProbe(ctx context.Context, stopCh <-chan struct{}, client *redis.Client, probeKeys []string,
	state *probeState, lastSuccessSeq, lastSuccessTS *atomic.Int64, errs *errorCollector) {
	ticker := time.NewTicker(cfg.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			// 时间戳在每次 Set 发起前取：故障期间单次 Set 最长阻塞 OpTimeout，
			// 若在调用返回后才取时间，测得的故障/恢复时刻会被阻塞时长系统性放大。
			// 失败分支必须用「失败那个 key」的发起时刻——多 key 探测时前面的 key
			// 可能成功耗时，统一用首个 key 的时刻会把故障起点系统性提前。
			probeStart := time.Now()
			failTime := time.Time{}
			for _, pk := range probeKeys {
				attemptStart := time.Now()
				if err := setWithTimeout(ctx, client, pk, attemptStart.UnixMilli(), time.Minute); err != nil {
					failTime = attemptStart
					errs.record(err)
					break
				}
			}
			state.totalProbeAttempts.Add(1)

			// msg 在锁内组装、锁外打印：stdout 阻塞（管道消费慢/终端流控）时
			// 不拉长锁占用，避免延迟主循环观察恢复与报告生成。
			var msg string
			if failTime.IsZero() {
				state.successProbeCount.Add(1)
				msg = state.onProbeSuccess(probeStart)
			} else {
				state.probeFailCount.Add(1)
				msg = state.onProbeFailure(failTime, lastSuccessSeq, lastSuccessTS)
			}
			if msg != "" {
				fmt.Print(msg)
			}
		}
	}
}

// onProbeSuccess 处理一次全 key 成功的探测。consecOK/consecFail 的重置全部收在
// 状态机方法内，与 onProbeFailure 对称，保证两方法可脱离 startProbe 独立测试。
func (s *probeState) onProbeSuccess(attemptStart time.Time) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 无条件清零：恢复态下「失败-成功-失败-失败」若不清零会凑满非连续失败
	// 触发误撤销，破坏 fail-threshold 去抖语义
	s.consecFail = 0
	if s.recovered {
		return ""
	}
	s.firstFailCandidate = time.Time{}
	s.consecOK++
	if s.consecOK < cfg.RecoverSuccessN || !s.recoverTime.IsZero() || s.firstFailTime.IsZero() {
		return ""
	}
	s.recoverTime = attemptStart
	s.recovered = true
	return fmt.Sprintf("[RECOVER] 连续 %d 次成功，判定恢复 @ %s\n",
		cfg.RecoverSuccessN, s.recoverTime.Format("15:04:05.000"))
}

func (s *probeState) onProbeFailure(attemptStart time.Time, lastSuccessSeq, lastSuccessTS *atomic.Int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecFail++
	s.consecOK = 0

	// 恢复判定后观察期内再现连续失败：撤销本次恢复，交回主循环等待下一次判定，
	// 使 --post-recover-observe 真正起到「确保恢复稳定」的宣称作用。
	// 注意 firstFailTime 保留原值：本工具按单故障事件测量，撤销后再次恢复时
	// RTO 口径为「首次故障 → 最终稳定恢复」，flap 期间的时间全部计入 RTO。
	if s.recovered {
		if s.consecFail < cfg.FailThreshold {
			return ""
		}
		s.recoverTime = time.Time{}
		s.recovered = false
		s.consecFail = 0
		s.invalidations.Add(1)
		return fmt.Sprintf("[REVOKE] 恢复后观察期内连续 %d 次失败，撤销恢复判定（第 %d 次），继续等待\n",
			cfg.FailThreshold, s.invalidations.Load())
	}

	if s.consecFail == 1 {
		s.firstFailCandidate = attemptStart
	}
	if s.consecFail < cfg.FailThreshold || !s.firstFailTime.IsZero() {
		return ""
	}
	if !s.firstFailCandidate.IsZero() {
		s.firstFailTime = s.firstFailCandidate
	} else {
		s.firstFailTime = attemptStart
	}
	s.firstFailCandidate = time.Time{}
	// 冻结故障前最后成功写入快照，作为 RPO 存活扫描的上界与丢失量基准
	s.seqAtFault.Store(lastSuccessSeq.Load())
	s.tsAtFault.Store(lastSuccessTS.Load())
	return fmt.Sprintf("[AUTO-DETECT] 连续 %d 次失败，确认故障开始 @ %s（快照 last_seq=%d）\n",
		s.consecFail, s.firstFailTime.Format("15:04:05.000"), s.seqAtFault.Load())
}
