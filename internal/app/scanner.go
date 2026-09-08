package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// getBatchSize 批量 GET 的 pipeline 分块大小，平衡单批 RTT 与超时风险
const getBatchSize = 500

// checkScanIntegrity 校验 Proxy 的 SCAN/KEYS 是否支持全局扫描
func checkScanIntegrity(ctx context.Context, client *redis.Client) error {
	testPrefix := fmt.Sprintf("%s:%s:scan_check:", cfg.KeyPrefix, cfg.TestID)
	testKeys := []string{
		testPrefix + "a",
		testPrefix + "b",
		testPrefix + "c",
		testPrefix + "{1}:d",
		testPrefix + "{2}:e",
		testPrefix + "{3}:f",
	}

	// defer 必须先于写入循环注册：任一 Set 失败即 return 时，已写入的 key 也能被清理，
	// 否则残留的 scan_check key 会被后续 prefix:* 清理/扫描模式匹配到
	defer func() {
		for _, k := range testKeys {
			if err := client.Del(ctx, k).Err(); err != nil {
				log.Printf("清理检查 key %s 失败: %v", k, err)
			}
		}
	}()

	for _, k := range testKeys {
		if err := client.Set(ctx, k, "check", time.Minute).Err(); err != nil {
			return fmt.Errorf("写入测试 key 失败: %w", err)
		}
	}

	keys, err := scanKeys(ctx, client, testPrefix+"*")
	if err != nil {
		return err
	}
	found := make(map[string]bool, len(keys))
	for _, k := range keys {
		found[k] = true
	}

	missing := 0
	for _, k := range testKeys {
		if !found[k] {
			missing++
		}
	}
	if missing > 0 {
		return fmt.Errorf("扫描不完整，丢失 %d/%d 个测试 key。Proxy 可能不支持全局 SCAN/KEYS，RPO 结果将不可靠", missing, len(testKeys))
	}
	return nil
}

// scanKeys 按配置的 ScanMethod 取回匹配 pattern 的全部 key。
// 任一命令失败即返回错误而非部分结果：不完整的 key 集合会让 RPO 被高估且无法察觉。
func scanKeys(ctx context.Context, client *redis.Client, pattern string) ([]string, error) {
	switch cfg.ScanMethod {
	case "keys":
		keys, err := client.Keys(ctx, pattern).Result()
		if err != nil {
			return nil, fmt.Errorf("KEYS 命令失败: %w", err)
		}
		return keys, nil
	default:
		// SCAN 契约允许同一 key 多次返回（游标迭代期间 rehash 会重发）；
		// 云 Proxy 聚合多分片扫描时重复更显著——腾讯云 3 分片集群实测每个 key
		// 返回约 3 次。去重是调用方责任，否则 GET/DEL 次数与日志计数全部虚高。
		seen := make(map[string]struct{})
		var keys []string
		var cursor uint64
		for {
			batch, next, err := client.Scan(ctx, cursor, pattern, 300).Result()
			if err != nil {
				return nil, fmt.Errorf("SCAN 命令失败: %w", err)
			}
			for _, k := range batch {
				if _, dup := seen[k]; !dup {
					seen[k] = struct{}{}
					keys = append(keys, k)
				}
			}
			cursor = next
			if cursor == 0 {
				return keys, nil
			}
		}
	}
}

// hasKeys 探测 pattern 下是否存在 key，强制走游标 SCAN、命中第一批即返回。
// 与 scanKeys 不同，它不受 cfg.ScanMethod 影响：启动期的遗留检查若在共享大实例上
// 执行 KEYS 会阻塞整个 Redis，存在性判断只需 SCAN 首批即可，无需取回全量。
func hasKeys(ctx context.Context, client *redis.Client, pattern string) (bool, error) {
	var cursor uint64
	for {
		batch, next, err := client.Scan(ctx, cursor, pattern, 300).Result()
		if err != nil {
			return false, fmt.Errorf("SCAN 命令失败: %w", err)
		}
		if len(batch) > 0 {
			return true, nil
		}
		cursor = next
		if cursor == 0 {
			return false, nil
		}
	}
}

// scanTimeout 按预估 key 量动态设置扫描超时：30s 基础 + 每批 GET 1s，上限 10 分钟。
// 固定 30s 在大量 seq key 下会让串行扫描中途超时，产出偏低且不可信的 maxSeq。
func scanTimeout(estimatedKeys int64) time.Duration {
	const base = 30 * time.Second
	chunks := (estimatedKeys + getBatchSize - 1) / getBatchSize
	return min(base+time.Duration(chunks)*time.Second, 10*time.Minute)
}

// scanMaxSeq 扫描存活 seq key，求 upperBound（故障前最后成功序号快照）以内
// 的最大序号与时间戳以计算 RPO。
// 必须以上界过滤：writer 在恢复后会继续写入更大的 seq，若不过滤，扫描所得
// maxSeq 与运行结束的 lastSuccessSeq 恒等，丢失量恒为 0，RPO 指标失效。
// 返回错误表示扫描不完整（SCAN/KEYS/GET 失败或超时），此时结果不可用于 RPO 计算；
// redis.Nil（key 在 SCAN 与 GET 之间过期）属正常情况，仅计数不视为错误。
func scanMaxSeq(ctx context.Context, client *redis.Client, upperBound int64) (maxSeq int64, maxTS int64, err error) {
	pattern := cfg.KeyPrefix + ":" + cfg.TestID + ":seq:*"
	keys, err := scanKeys(ctx, client, pattern)
	if err != nil {
		return 0, 0, err
	}

	var expired, malformed, outOfRange int
	for chunk := range slices.Chunk(keys, getBatchSize) {
		cmds, perr := client.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, k := range chunk {
				p.Get(ctx, k)
			}
			return nil
		})
		// 整批失败：仅当非 redis.Nil（全部 key 恰好过期）时视为真实错误
		if perr != nil && !errors.Is(perr, redis.Nil) {
			return 0, 0, fmt.Errorf("批量 GET 失败: %w", perr)
		}
		for _, cmd := range cmds {
			sc, ok := cmd.(*redis.StringCmd)
			if !ok {
				continue
			}
			val, gerr := sc.Result()
			if errors.Is(gerr, redis.Nil) {
				expired++
				continue
			}
			if gerr != nil {
				return 0, 0, fmt.Errorf("读取 key 失败: %w", gerr)
			}
			tsStr, seqStr, cutOK := strings.Cut(val, "|")
			if !cutOK {
				malformed++
				continue
			}
			ts, errTS := strconv.ParseInt(tsStr, 10, 64)
			seq, errSeq := strconv.ParseInt(seqStr, 10, 64)
			if errTS != nil || errSeq != nil {
				malformed++
				continue
			}
			if seq > upperBound {
				outOfRange++
				continue
			}
			if seq > maxSeq {
				maxSeq = seq
				maxTS = ts
			}
		}
	}
	if expired > 0 || malformed > 0 || outOfRange > 0 {
		log.Printf("[WARN] 扫描 %d 个 key：%d 个已过期、%d 个格式异常、%d 个为故障后写入（不计入 RPO）",
			len(keys), expired, malformed, outOfRange)
	}
	return maxSeq, maxTS, nil
}

// computeRPO 以故障快照为基准计算丢失量：lost = 故障前最后成功序号 − 存活最大序号(≤快照)。
// valid=false 时 lost/rpoSec 无意义，reason 说明原因；纯函数以便穷举测试全部分支。
func computeRPO(seqAtFault, tsAtFault, maxSeq, maxTS int64, scanErr error) (lost int64, rpoSec *float64, valid bool, reason string) {
	if scanErr != nil {
		return 0, nil, false, fmt.Sprintf("扫描失败: %v", scanErr)
	}
	if seqAtFault <= 0 {
		return 0, nil, false, "故障前无成功写入（或故障未确认），无法计算丢失量"
	}
	lost = seqAtFault - maxSeq
	if lost < 0 {
		lost = 0
	}
	if tsAtFault > maxTS && maxTS > 0 {
		rpoSec = new(float64(tsAtFault-maxTS) / 1000.0)
	}
	return lost, rpoSec, true, ""
}

// cleanTestKeys 删除历次运行遗留的测试 key（seq/health/scan_check）。
// 清理范围为 prefix:* 跨 TestID：本次 TestID 在 Redis 中必无历史 key，跨 TestID 才能清到遗留数据。
// 前提假设：同一 Redis 实例同一时刻只运行一个本工具进程；
// 若同 prefix 并行运行多个进程，后启动者的 --clean 会误删在跑进程的测量数据。
// 返回删除/失败计数与扫描错误，由调用方决定是否继续测试。
func cleanTestKeys(ctx context.Context, client *redis.Client) (deleted, failed int, err error) {
	keys, err := scanKeys(ctx, client, cfg.KeyPrefix+":*")
	if err != nil {
		return 0, 0, err
	}
	for chunk := range slices.Chunk(keys, getBatchSize) {
		cmds, perr := client.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, k := range chunk {
				p.Del(ctx, k)
			}
			return nil
		})
		if perr != nil {
			return deleted, failed + len(chunk), fmt.Errorf("批量 DEL 失败: %w", perr)
		}
		for _, cmd := range cmds {
			if cerr := cmd.Err(); cerr != nil {
				log.Printf("删除 key 失败: %v", cerr)
				failed++
			} else {
				deleted++
			}
		}
	}
	return deleted, failed, nil
}
