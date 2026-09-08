package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestScanTimeoutDynamic 锁定动态扫描超时：按 key 量放大、10 分钟封顶，
// 防止回到固定 30s 导致大量 seq key 下扫描中途超时产出不可信 maxSeq。
func TestScanTimeoutDynamic(t *testing.T) {
	cases := []struct {
		name string
		keys int64
		want time.Duration
	}{
		{"空集用基础值", 0, 30 * time.Second},
		{"一批以内加1s", getBatchSize, 31 * time.Second},
		{"两批加2s", 2 * getBatchSize, 32 * time.Second},
		{"超量封顶10分钟", 10_000_000, 10 * time.Minute},
	}
	for _, c := range cases {
		if got := scanTimeout(c.keys); got != c.want {
			t.Errorf("%s: scanTimeout(%d) = %s，期望 %s", c.name, c.keys, got, c.want)
		}
	}
}

// TestConfigOpTimeoutValidation 锁定 op-timeout 校验：单次命令超时是故障判定精度的
// 实际边界，过大（超过 probe-interval 10 倍）会让 socket 阻塞主导探测间隔。
func TestConfigOpTimeoutValidation(t *testing.T) {
	base := func() Config {
		return Config{
			Addr: "127.0.0.1:6379", KeyPrefix: "p", TestID: "t",
			ScanMethod: "scan", WriteInterval: time.Millisecond, ProbeInterval: 100 * time.Millisecond,
			StatusInterval: time.Second, MaxRunTime: time.Minute, PostRecoverObserve: time.Second,
			ProbeKeyCount: 1, RecoverSuccessN: 1, FailThreshold: 1, PoolSize: 1,
			OpTimeout: 200 * time.Millisecond,
		}
	}
	baseCfg := base()
	if err := baseCfg.Validate(); err != nil {
		t.Fatalf("基准配置应通过校验: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"零值拒绝", func(c *Config) { c.OpTimeout = 0 }, "op-timeout 必须为正值"},
		{"负值拒绝", func(c *Config) { c.OpTimeout = -time.Second }, "op-timeout 必须为正值"},
		{"超绝对上界拒绝", func(c *Config) { c.OpTimeout = 31 * time.Second }, "不能超过"},
		{"max-run超24h拒绝", func(c *Config) { c.MaxRunTime = 25 * time.Hour }, "max-run 不能超过"},
	}
	for _, c := range cases {
		cfg := base()
		c.mutate(&cfg)
		err := cfg.Validate()
		if err == nil {
			t.Errorf("%s: 期望报错 %q，实际通过", c.name, c.wantErr)
		} else if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: 期望错误含 %q，实际 %q", c.name, c.wantErr, err.Error())
		}
	}
}

// TestComputeRPO 穷举 RPO 计算全部分支，锁定 critical finding 的核心口径：
// 以故障快照 seqAtFault 为基准，而非运行结束的 lastSuccessSeq（后者会让 lost 恒为 0）。
func TestComputeRPO(t *testing.T) {
	errScan := errors.New("SCAN 命令失败")
	cases := []struct {
		name       string
		seqAtFault int64
		tsAtFault  int64
		maxSeq     int64
		maxTS      int64
		scanErr    error
		wantLost   int64
		wantValid  bool
		wantSec    bool
		wantReason string
	}{
		{"扫描失败标记无效", 100, 2000, 50, 1000, errScan, 0, false, false, "扫描失败: SCAN 命令失败"},
		{"快照为0（故障前无成功写入）", 0, 0, 0, 0, nil, 0, false, false, "故障前无成功写入"},
		{"故障窗口内丢失30条", 100, 2000, 70, 1400, nil, 30, true, true, ""},
		{"存活到快照（零丢失）", 100, 2000, 100, 2000, nil, 0, true, false, ""},
		{"存活超过快照钳为0", 100, 2000, 120, 2500, nil, 0, true, false, ""},
		{"全部丢失（存活0）", 100, 2000, 0, 0, nil, 100, true, false, ""},
		{"时间戳不回退则无窗口", 100, 2000, 70, 2000, nil, 30, true, false, ""},
	}
	for _, c := range cases {
		lost, rpoSec, valid, reason := computeRPO(c.seqAtFault, c.tsAtFault, c.maxSeq, c.maxTS, c.scanErr)
		if lost != c.wantLost || valid != c.wantValid {
			t.Errorf("%s: got lost=%d valid=%v，期望 lost=%d valid=%v", c.name, lost, valid, c.wantLost, c.wantValid)
		}
		if (rpoSec != nil) != c.wantSec {
			t.Errorf("%s: rpoSec 非nil=%v，期望 %v", c.name, rpoSec != nil, c.wantSec)
		}
		if c.wantReason != "" && !strings.Contains(reason, c.wantReason) {
			t.Errorf("%s: reason=%q，期望含 %q", c.name, reason, c.wantReason)
		}
	}
}

// TestComputeRPOUsesFaultSnapshotNotFinalSeq 复现 critical finding 的失效场景：
// 恢复后 writer 继续写大 seq，若用运行结束的 lastSuccessSeq(=200) 作基准，
// 与扫描所得 maxSeq 恒等 → lost=0；改用故障快照 seqAtFault(=100) 才能测出真实丢失。
func TestComputeRPOUsesFaultSnapshotNotFinalSeq(t *testing.T) {
	const lastSuccessSeqAtEnd = 200 // 恢复后继续写入的最终序号
	const seqAtFault = 100          // 故障确认时冻结的快照
	const maxSeqSurvived = 70       // 扫描得到：故障窗口内存活的最大 seq（≤快照）

	// 错误口径（旧实现）：以最终序号为基准，丢失被恢复后写入掩盖
	wrongLost, _, _, _ := computeRPO(lastSuccessSeqAtEnd, 0, maxSeqSurvived, 0, nil)
	// 正确口径：以故障快照为基准
	gotLost, _, valid, _ := computeRPO(seqAtFault, 0, maxSeqSurvived, 0, nil)

	if !valid {
		t.Fatal("快照口径应判定 RPO 有效")
	}
	if gotLost != seqAtFault-maxSeqSurvived {
		t.Errorf("快照口径 lost=%d，期望 %d", gotLost, seqAtFault-maxSeqSurvived)
	}
	if wrongLost != gotLost {
		t.Logf("对照确认：错误口径 lost=%d ≠ 正确口径 lost=%d（旧实现的 RPO 失效已被修复）", wrongLost, gotLost)
	}
}

// TestScanMaxSeqFailsOnUnreachableRedis 锁定 finding 核心契约：扫描失败必须返回
// error 而非静默产出 maxSeq=0 的假数据（旧实现会让调用方把扫描失败误判为全部丢失）。
func TestScanMaxSeqFailsOnUnreachableRedis(t *testing.T) {
	old := cfg
	defer func() { cfg = old }()
	cfg.KeyPrefix, cfg.TestID, cfg.ScanMethod, cfg.OpTimeout = "p", "t", "scan", 200*time.Millisecond

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer client.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if _, _, err := scanMaxSeq(ctx, client, 1000); err == nil {
		t.Error("scanMaxSeq 对不可达实例应返回错误，而非静默产出假数据")
	}
	if _, _, err := cleanTestKeys(ctx, client); err == nil {
		t.Error("cleanTestKeys 对不可达实例应返回错误，而非假装清理完成")
	}
}

// TestResultJSONRPOValidity 锁定 RPO 有效性契约：无效时 rpo_valid=false 且带原因，
// 有效时原因省略；消费方据此区分「实测零丢失」与「扫描失败未计算」。
func TestResultJSONRPOValidity(t *testing.T) {
	invalid, err := json.Marshal(TestResult{TestID: "t1", RPOValid: false, RPOInvalidReason: "扫描失败"})
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	if !strings.Contains(string(invalid), `"rpo_valid":false`) ||
		!strings.Contains(string(invalid), `"rpo_invalid_reason":"扫描失败"`) {
		t.Errorf("RPO 无效时应输出 false 与原因，实际: %s", invalid)
	}

	valid, err := json.Marshal(TestResult{TestID: "t1", RPOValid: true})
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	if !strings.Contains(string(valid), `"rpo_valid":true`) {
		t.Errorf("RPO 有效时应输出 true，实际: %s", valid)
	}
	if strings.Contains(string(valid), "rpo_invalid_reason") {
		t.Errorf("RPO 有效时原因应省略，实际: %s", valid)
	}
}
