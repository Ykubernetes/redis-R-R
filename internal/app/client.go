package app

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// newClient 创建测量专用的 Redis 客户端
func newClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		PoolSize: cfg.PoolSize,
		// MinIdleConns=0：测量客户端不预热空闲连接。go-redis 会在连接被摘除后用
		// context.Background() 后台补足最小空闲连接，产生不计入测量路径的重连流量，
		// 并可能提前重建热连接使探测跳过真实拨号代价，令故障时刻偏乐观。
		MinIdleConns: 0,
		MaxRetries:   -1, // -1 禁用重试，失败原样暴露，避免稀释故障时间戳
		// 拨号与读写超时统一由 OpTimeout 约束，避免 socket 层默认值成为隐式精度上限
		DialTimeout:  cfg.OpTimeout,
		ReadTimeout:  cfg.OpTimeout,
		WriteTimeout: cfg.OpTimeout,
		// ContextTimeoutEnabled 让单次命令的 ctx deadline 传导到池化连接，
		// 否则 ctx 取消/期限对已建立连接不生效，超时只能依赖上面的 socket 值。
		ContextTimeoutEnabled: true,
	})
}

// setWithTimeout 执行单次 SET，用 OpTimeout 显式约束耗时上限。
// 配合 ContextTimeoutEnabled，使单次探测/写入的阻塞时间可控——这是故障时刻判定精度的实际边界。
func setWithTimeout(ctx context.Context, client *redis.Client, key string, value any, ttl time.Duration) error {
	opCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
	defer cancel()
	return client.Set(opCtx, key, value, ttl).Err()
}
