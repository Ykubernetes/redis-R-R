package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// startWriter 按固定间隔写入 seq key，并记录最后成功的序号与时间戳
func startWriter(ctx context.Context, stopCh <-chan struct{}, client *redis.Client,
	totalWriteAttempts, successWriteCount, lastSuccessSeq, lastSuccessTS *atomic.Int64,
	errs *errorCollector) {

	ticker := time.NewTicker(cfg.WriteInterval)
	defer ticker.Stop()
	// seq 为本 goroutine 独占的局部变量，无并发访问；跨 goroutine 的共享状态
	// 只有写入成功后的 lastSuccessSeq/lastSuccessTS（atomic）
	var seq int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			seq++
			s := seq
			// 用发起时刻而非 ticker 计划触发时刻：前次 Set 阻塞时后续 tick 被丢弃，
			// 恢复后收到的是陈旧触发时刻，会令 ts/lastSuccessTS 比真实写入早数秒。
			// 该 ts 同时写入 val 与 lastSuccessTS，保证与扫描读回的 maxTS 同口径。
			ts := time.Now().UnixMilli()
			key := fmt.Sprintf("%s:%s:seq:%d", cfg.KeyPrefix, cfg.TestID, s)
			val := fmt.Sprintf("%d|%d", ts, s)

			// TTL 随本次运行时长派生：硬编码 90 分钟在长 --max-run 下会让故障前的
			// seq key 在扫描前过期，maxSeq 归零、丢失量虚增为全部写入
			err := setWithTimeout(ctx, client, key, val, cfg.MaxRunTime+30*time.Minute)
			totalWriteAttempts.Add(1)

			if err == nil {
				successWriteCount.Add(1)
				lastSuccessSeq.Store(s)
				lastSuccessTS.Store(ts)
			} else {
				errs.record(err)
			}
		}
	}
}
