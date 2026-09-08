package app

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

// Config 工具运行配置，由命令行参数填充
type Config struct {
	Addr               string // 地址，格式 host:port
	Password           string // 连接密码，敏感字段，禁止加入任何日志或 JSON 输出路径
	KeyPrefix          string
	WriteInterval      time.Duration
	ProbeInterval      time.Duration
	OpTimeout          time.Duration // 单次探测/写入命令的超时上限，决定故障时刻判定精度
	RecoverSuccessN    int
	FailThreshold      int
	ProbeKeyCount      int
	MaxRunTime         time.Duration
	PostRecoverObserve time.Duration
	PoolSize           int
	CleanBeforeStart   bool
	StatusInterval     time.Duration
	OutputJSON         string
	ScanMethod         string
	SkipScanCheck      bool
	TestID             string // 测试唯一标识，默认使用时间戳
}

// 参数上限：超限会在下游造成 int32 截断或巨额内存分配，须在启动期拦截
const (
	maxProbeKeyCount = 4096
	maxPoolSize      = 4096
	maxThreshold     = math.MaxInt32
	// OpTimeout 上界：单次命令超时须远小于扫描上下文，避免一次探测阻塞整轮
	maxOpTimeout = 30 * time.Second
	// MaxRunTime 上界：seq key TTL 随其派生（MaxRunTime+30m），无上界会让
	// 极端值给 Redis 带来无界内存占用
	maxRunTimeLimit = 24 * time.Hour
)

var cfg Config

// Validate 在启动阶段校验全部配置，任一非法即返回错误，避免运行期 panic 或测量失真
func (c *Config) Validate() error {
	// Addr 直接交给 go-redis，空值会静默回退 localhost:6379，可能连上非预期实例执行写入/清理
	if c.Addr == "" {
		return fmt.Errorf("addr 不能为空")
	}
	host, port, err := net.SplitHostPort(c.Addr)
	if err != nil {
		return fmt.Errorf("非法的 addr: %q（应为 host:port）: %w", c.Addr, err)
	}
	if host == "" || strings.Contains(host, "/") {
		return fmt.Errorf("非法的 addr: %q（host 无效，勿带 redis:// 等 scheme）", c.Addr)
	}
	if p, e := strconv.Atoi(port); e != nil || p < 1 || p > 65535 {
		return fmt.Errorf("非法的 addr: %q（端口须在 1-65535）", c.Addr)
	}

	if c.ScanMethod != "scan" && c.ScanMethod != "keys" {
		return fmt.Errorf("不支持的 scan-method: %s（仅支持 scan 或 keys）", c.ScanMethod)
	}
	// 间隔类参数直接喂给 time.NewTicker / time.After，非正值会 panic 或立即就绪
	if c.WriteInterval <= 0 {
		return fmt.Errorf("write-interval 必须为正值，当前为 %s", c.WriteInterval)
	}
	if c.ProbeInterval <= 0 {
		return fmt.Errorf("probe-interval 必须为正值，当前为 %s", c.ProbeInterval)
	}
	if c.StatusInterval <= 0 {
		return fmt.Errorf("status-interval 必须为正值，当前为 %s", c.StatusInterval)
	}
	if c.MaxRunTime <= 0 {
		return fmt.Errorf("max-run 必须为正值，当前为 %s", c.MaxRunTime)
	}
	if c.MaxRunTime > maxRunTimeLimit {
		return fmt.Errorf("max-run 不能超过 %s（seq key TTL 随其派生），当前为 %s", maxRunTimeLimit, c.MaxRunTime)
	}
	if c.PostRecoverObserve <= 0 {
		return fmt.Errorf("post-recover-observe 必须为正值，当前为 %s", c.PostRecoverObserve)
	}
	// OpTimeout 是单次探测/写入的耗时上限，直接决定故障时刻判定精度；
	// 过大会让 RTO 被 socket 阻塞系统性放大，非正值会让 context.WithTimeout 立即到期
	if c.OpTimeout <= 0 {
		return fmt.Errorf("op-timeout 必须为正值，当前为 %s", c.OpTimeout)
	}
	if c.OpTimeout > maxOpTimeout {
		return fmt.Errorf("op-timeout 不能超过 %s，当前为 %s", maxOpTimeout, c.OpTimeout)
	}
	// 阈值与池参数：下界保证检测有效，上界防止 int32 截断与巨额分配
	if c.ProbeKeyCount < 1 || c.ProbeKeyCount > maxProbeKeyCount {
		return fmt.Errorf("probe-keys 必须在 [1, %d]，当前为 %d", maxProbeKeyCount, c.ProbeKeyCount)
	}
	if c.RecoverSuccessN < 1 || c.RecoverSuccessN > maxThreshold {
		return fmt.Errorf("recover-n 必须在 [1, %d]，当前为 %d", maxThreshold, c.RecoverSuccessN)
	}
	if c.FailThreshold < 1 || c.FailThreshold > maxThreshold {
		return fmt.Errorf("fail-threshold 必须在 [1, %d]，当前为 %d", maxThreshold, c.FailThreshold)
	}
	if c.PoolSize < 1 || c.PoolSize > maxPoolSize {
		return fmt.Errorf("pool-size 必须在 [1, %d]，当前为 %d", maxPoolSize, c.PoolSize)
	}

	// KeyPrefix 与 TestID 都会被拼进 SCAN/KEYS 的 glob，通配符会令清理/扫描范围失控
	if !isSafeToken(c.KeyPrefix) {
		return fmt.Errorf("非法的 prefix: %q（仅允许字母、数字、下划线、连字符、点，且不能为空）", c.KeyPrefix)
	}
	if !isSafeToken(c.TestID) {
		return fmt.Errorf("非法的 test-id: %q（仅允许字母、数字、下划线、连字符、点，且不能为空）", c.TestID)
	}

	return nil
}

// maskPassword 屏蔽密码用于展示，空值保持空以区分「未设置密码」
func maskPassword(p string) string {
	if p == "" {
		return ""
	}
	return "***"
}

// String 实现 fmt.Stringer，使 %v/%s/%+v 输出屏蔽 Password，防止调试日志泄漏
func (c Config) String() string {
	type plain Config
	p := plain(c)
	p.Password = maskPassword(c.Password)
	return fmt.Sprintf("%+v", p)
}

// GoString 实现 fmt.GoStringer，使 %#v 输出屏蔽 Password
func (c Config) GoString() string {
	type plain Config
	p := plain(c)
	p.Password = maskPassword(c.Password)
	return fmt.Sprintf("%#v", p)
}

// MarshalJSON 使 json.Marshal(Config) 屏蔽 Password，防止整体序列化泄漏
func (c Config) MarshalJSON() ([]byte, error) {
	type plain Config
	p := plain(c)
	p.Password = maskPassword(c.Password)
	return json.Marshal(p)
}
