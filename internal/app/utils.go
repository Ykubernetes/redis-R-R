package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// classifyError 将错误归类为统计 key（统一小写）。
// 判定顺序即优先级：上层语义（ctx / 连接池）> RESP 协议错误 > 网络层错误 > 文本兜底。
func classifyError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	// 上下文取消/超时优先判定：runTest 退出时在途命令正是以此失败
	case errors.Is(err, context.Canceled):
		return "ctx_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "ctx_deadline"
	// 池耗尽文本含 "timeout"，必须先于网络超时判定，避免掩盖池不足问题
	case errors.Is(err, redis.ErrPoolTimeout):
		return "pool_timeout"
	case errors.Is(err, redis.Nil):
		// 键不存在，非故障，单列以免计入 other
		return "nil_key"
	}

	// RESP 协议错误：对应 go-redis parseTypedRedisError 的全部分派分支，
	// 每个 Is*Error 判定一种 Redis 错误码前缀。
	switch {
	case redis.IsLoadingError(err):
		return "loading"
	case redis.IsReadOnlyError(err):
		return "readonly"
	case redis.IsClusterDownError(err):
		return "clusterdown"
	case redis.IsNoReplicasError(err):
		return "noreplicas"
	case redis.IsMasterDownError(err):
		return "masterdown"
	case redis.IsTryAgainError(err):
		return "tryagain"
	case redis.IsMaxClientsError(err):
		return "maxclients"
	case redis.IsAuthError(err):
		return "auth"
	case redis.IsPermissionError(err):
		return "noperm"
	case redis.IsExecAbortError(err):
		return "execabort"
	case redis.IsOOMError(err):
		return "oom"
	}
	// MOVED/ASK 返回 (addr, ok) 而非 bool，无法并入上面的 switch
	if _, ok := redis.IsMovedError(err); ok {
		return "moved"
	}
	if _, ok := redis.IsAskError(err); ok {
		return "ask"
	}
	// 兜底：未被上面类型化的 RESP 错误（如 WRONGTYPE、各云厂商自定义错误码）
	// 都实现 redis.Error 接口，据此与网络层错误区分。
	if _, ok := errors.AsType[redis.Error](err); ok {
		return "proto_error"
	}

	// 网络层错误：类型判定未命中时，用标准库接口与 errno 归类
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		return "timeout"
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	// 客户端主动关闭连接，非 syscall errno
	case errors.Is(err, redis.ErrClosed), errors.Is(err, net.ErrClosed):
		return "conn_closed"
	// 文本匹配仅作兜底，防 go-redis 版本变化引入新错误文本
	case strings.Contains(err.Error(), "timeout"):
		return "timeout"
	case strings.Contains(err.Error(), "connection refused"):
		return "connection_refused"
	}
	return "other"
}

// errorSample 记录一类错误的原始形态：Go 类型、错误文本、出现次数。
// 分类器无法穷举各云厂商的错误码，样本用于事后核实归类是否准确。
type errorSample struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	Count int64  `json:"count"`
}

const (
	maxSamplesPerBucket = 5
	maxSampleTextLen    = 256
)

// errorCollector 并发安全地统计错误分类，并为每个桶保留去重后的样本
type errorCollector struct {
	mu      sync.Mutex
	counts  map[string]int64
	samples map[string][]errorSample
}

// newErrorCollector 创建空的错误收集器
func newErrorCollector() *errorCollector {
	return &errorCollector{
		counts:  make(map[string]int64),
		samples: make(map[string][]errorSample),
	}
}

// record 归类单个错误并累计样本，nil 错误忽略。
// 样本按 Go 类型与文本去重：同一形态重复出现只加计数，不新增条目。
func (c *errorCollector) record(err error) {
	if err == nil {
		return
	}
	key := classifyError(err)
	typeName := fmt.Sprintf("%T", err)
	text := truncateSampleText(err.Error())

	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key]++

	bucket := c.samples[key]
	for i := range bucket {
		if bucket[i].Type == typeName && bucket[i].Text == text {
			bucket[i].Count++
			return
		}
	}
	if len(bucket) < maxSamplesPerBucket {
		c.samples[key] = append(bucket, errorSample{Type: typeName, Text: text, Count: 1})
	}
}

// Stats 返回分类计数快照，供报告与 JSON 输出使用
func (c *errorCollector) Stats() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

// Samples 返回每桶样本快照，按计数降序、文本升序排列，保证输出稳定可复现
func (c *errorCollector) Samples() map[string][]errorSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]errorSample, len(c.samples))
	for k, v := range c.samples {
		cp := make([]errorSample, len(v))
		copy(cp, v)
		sort.Slice(cp, func(i, j int) bool {
			if cp[i].Count != cp[j].Count {
				return cp[i].Count > cp[j].Count
			}
			return cp[i].Text < cp[j].Text
		})
		out[k] = cp
	}
	return out
}

// truncateSampleText 按 rune 截断错误文本，避免多字节字符被切成半个
func truncateSampleText(s string) string {
	if len(s) <= maxSampleTextLen {
		return s
	}
	r := []rune(s)
	if len(r) <= maxSampleTextLen {
		return s
	}
	return string(r[:maxSampleTextLen]) + "..."
}

// formatTime 格式化时间，零值返回 N/A
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "N/A"
	}
	return t.Format("2006-01-02 15:04:05.000")
}

// isSafeToken 校验会拼进 SCAN/KEYS glob 的字段，禁止含通配符以免清理范围失控
func isSafeToken(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.':
		default:
			return false
		}
	}
	return true
}
