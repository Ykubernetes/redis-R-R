package app

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// probeTestBase 提供可区分的探测时刻序列
var probeTestBase = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func probeT(i int) time.Time { return probeTestBase.Add(time.Duration(i) * time.Second) }

func withProbeCfg(t *testing.T, failThreshold, recoverN int) {
	old := cfg
	t.Cleanup(func() { cfg = old })
	cfg.FailThreshold = failThreshold
	cfg.RecoverSuccessN = recoverN
}

// confirmFault 驱动状态机走到「故障已确认」，返回确认消息
func confirmFault(t *testing.T, s *probeState, seq, ts *atomic.Int64) {
	t.Helper()
	for i := 1; i <= cfg.FailThreshold; i++ {
		s.onProbeFailure(probeT(i), seq, ts)
	}
	s.mu.Lock()
	confirmed := !s.firstFailTime.IsZero()
	s.mu.Unlock()
	if !confirmed {
		t.Fatalf("前置条件失败：%d 次连续失败后应确认故障", cfg.FailThreshold)
	}
}

// recoverFrom 驱动状态机走到「已判定恢复」
func recoverFrom(t *testing.T, s *probeState, startIdx int) {
	t.Helper()
	for i := range cfg.RecoverSuccessN {
		s.onProbeSuccess(probeT(startIdx + i))
	}
	if !s.isRecovered() {
		t.Fatalf("前置条件失败：%d 次连续成功后应判定恢复", cfg.RecoverSuccessN)
	}
}

// TestProbeFaultConfirmation 验证去抖确认：故障时刻取首次失败候选（而非确认时刻），
// 快照冻结当时的 lastSuccessSeq/TS。
func TestProbeFaultConfirmation(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64
	seq.Store(100)
	ts.Store(5000)

	if msg := s.onProbeFailure(probeT(1), &seq, &ts); msg != "" {
		t.Errorf("首次失败不应输出消息，实际 %q", msg)
	}
	s.onProbeFailure(probeT(2), &seq, &ts)
	msg := s.onProbeFailure(probeT(3), &seq, &ts)
	if !strings.Contains(msg, "AUTO-DETECT") {
		t.Errorf("第 3 次连续失败应确认故障并输出消息，实际 %q", msg)
	}

	s.mu.Lock()
	fft := s.firstFailTime
	s.mu.Unlock()
	if !fft.Equal(probeT(1)) {
		t.Errorf("故障时刻应为首次失败候选 %v，实际 %v", probeT(1), fft)
	}
	if got := s.seqAtFault.Load(); got != 100 {
		t.Errorf("快照 seqAtFault 应为 100，实际 %d", got)
	}
	if got := s.tsAtFault.Load(); got != 5000 {
		t.Errorf("快照 tsAtFault 应为 5000，实际 %d", got)
	}
}

// TestProbeDebounceResetBySuccess 验证非连续失败不触发确认：
// 失败-失败-成功-失败-失败-失败，确认时刻应为第二段连续失败的起点。
func TestProbeDebounceResetBySuccess(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64

	s.onProbeFailure(probeT(1), &seq, &ts)
	s.onProbeFailure(probeT(2), &seq, &ts)
	s.onProbeSuccess(probeT(3))
	s.onProbeFailure(probeT(4), &seq, &ts)
	s.onProbeFailure(probeT(5), &seq, &ts)

	s.mu.Lock()
	notYet := s.firstFailTime.IsZero()
	s.mu.Unlock()
	if !notYet {
		t.Fatal("2+2 次非连续失败不应确认故障")
	}

	s.onProbeFailure(probeT(6), &seq, &ts)
	s.mu.Lock()
	fft := s.firstFailTime
	s.mu.Unlock()
	if !fft.Equal(probeT(4)) {
		t.Errorf("确认时刻应为第二段连续失败起点 %v，实际 %v", probeT(4), fft)
	}
}

// TestProbeRecoveryAfterFault 验证恢复判定：故障确认后连续 recover-n 次成功，
// 恢复时刻取最后一次成功探测的发起时刻。
func TestProbeRecoveryAfterFault(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64
	confirmFault(t, &s, &seq, &ts)

	for i := range cfg.RecoverSuccessN - 1 {
		if msg := s.onProbeSuccess(probeT(10 + i)); msg != "" {
			t.Errorf("未满 %d 次成功不应输出恢复消息，实际 %q", cfg.RecoverSuccessN, msg)
		}
	}
	msg := s.onProbeSuccess(probeT(20))
	if !strings.Contains(msg, "RECOVER") {
		t.Errorf("第 %d 次成功应输出恢复消息，实际 %q", cfg.RecoverSuccessN, msg)
	}
	s.mu.Lock()
	rt := s.recoverTime
	s.mu.Unlock()
	if !rt.Equal(probeT(20)) {
		t.Errorf("恢复时刻应为最后一次成功探测时刻 %v，实际 %v", probeT(20), rt)
	}
	if !s.isRecovered() {
		t.Error("recovered 标志应置位")
	}
}

// TestProbeRevocationAfterRecovery 验证观察期撤销：恢复后连续 fail-threshold 次失败
// 撤销恢复判定并计数。
func TestProbeRevocationAfterRecovery(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64
	confirmFault(t, &s, &seq, &ts)
	recoverFrom(t, &s, 10)

	s.onProbeFailure(probeT(30), &seq, &ts)
	s.onProbeFailure(probeT(31), &seq, &ts)
	s.mu.Lock()
	stillSet := !s.recoverTime.IsZero()
	s.mu.Unlock()
	if !stillSet || s.invalidations.Load() != 0 {
		t.Fatal("未满 fail-threshold 不应撤销")
	}

	msg := s.onProbeFailure(probeT(32), &seq, &ts)
	if !strings.Contains(msg, "REVOKE") {
		t.Errorf("第 3 次连续失败应撤销并输出消息，实际 %q", msg)
	}
	s.mu.Lock()
	zeroed := s.recoverTime.IsZero()
	s.mu.Unlock()
	if !zeroed || s.isRecovered() {
		t.Error("撤销后 recoverTime 应清零且 recovered 应复位")
	}
	if got := s.invalidations.Load(); got != 1 {
		t.Errorf("invalidations 应为 1，实际 %d", got)
	}
}

// TestProbeReRecoveryRequiresFullCount 锁定撤销后的再恢复必须重新数满
// recover-n 次连续成功——单次成功不得立即恢复（consecOK 在每次失败时已清零）。
func TestProbeReRecoveryRequiresFullCount(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64
	confirmFault(t, &s, &seq, &ts)
	recoverFrom(t, &s, 10)
	for i := range cfg.FailThreshold {
		s.onProbeFailure(probeT(30+i), &seq, &ts)
	}

	if msg := s.onProbeSuccess(probeT(40)); msg != "" {
		t.Errorf("撤销后单次成功不应立即再恢复，实际输出 %q", msg)
	}
	s.mu.Lock()
	notYet := s.recoverTime.IsZero()
	s.mu.Unlock()
	if !notYet || s.isRecovered() {
		t.Fatal("撤销后单次成功不得置恢复")
	}

	for i := 1; i < cfg.RecoverSuccessN; i++ {
		s.onProbeSuccess(probeT(40 + i))
	}
	if !s.isRecovered() {
		t.Errorf("撤销后重新数满 %d 次成功应再恢复", cfg.RecoverSuccessN)
	}
}

// TestProbeNonConsecutiveFailsNoRevocation 锁定恢复态的去抖语义：
// 失败-成功-失败-失败（非连续）不得触发撤销。成功必须清零 consecFail，
// 否则残留计数会与后续失败凑满阈值造成误撤销。
func TestProbeNonConsecutiveFailsNoRevocation(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64
	confirmFault(t, &s, &seq, &ts)
	recoverFrom(t, &s, 10)

	s.onProbeFailure(probeT(30), &seq, &ts)
	s.onProbeSuccess(probeT(31))
	s.onProbeFailure(probeT(32), &seq, &ts)
	s.onProbeFailure(probeT(33), &seq, &ts)

	if got := s.invalidations.Load(); got != 0 {
		t.Errorf("非连续失败不应撤销，invalidations=%d", got)
	}
	if !s.isRecovered() {
		t.Error("恢复判定应保持有效")
	}
	s.mu.Lock()
	stillSet := !s.recoverTime.IsZero()
	s.mu.Unlock()
	if !stillSet {
		t.Error("recoverTime 应保持原值")
	}
}

// TestProbeSnapshotFrozenAtFirstConfirmation 验证快照只在首次确认时冻结一次：
// 后续失败与 lastSuccessSeq 变化都不得改写快照，否则 RPO 基准漂移。
func TestProbeSnapshotFrozenAtFirstConfirmation(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState
	var seq, ts atomic.Int64
	seq.Store(100)
	ts.Store(5000)
	confirmFault(t, &s, &seq, &ts)

	seq.Store(150) // 恢复后 writer 继续写入
	ts.Store(9000)
	for i := range 5 {
		s.onProbeFailure(probeT(30+i), &seq, &ts)
	}
	if got := s.seqAtFault.Load(); got != 100 {
		t.Errorf("快照应保持首次确认值 100，实际 %d", got)
	}
	if got := s.tsAtFault.Load(); got != 5000 {
		t.Errorf("快照应保持首次确认值 5000，实际 %d", got)
	}
}

// TestProbeNoRecoveryWithoutFault 验证从未确认故障时，再多成功也不判定恢复
// （无故障则无 RTO 可言）。
func TestProbeNoRecoveryWithoutFault(t *testing.T) {
	withProbeCfg(t, 3, 6)
	var s probeState

	for i := range cfg.RecoverSuccessN * 2 {
		if msg := s.onProbeSuccess(probeT(i)); msg != "" {
			t.Errorf("无故障时不应输出恢复消息，实际 %q", msg)
		}
	}
	if s.isRecovered() {
		t.Error("无故障时不得判定恢复")
	}
}
