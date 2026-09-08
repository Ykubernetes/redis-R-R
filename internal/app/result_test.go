package app

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestResultJSONNilOmitted 锁定核心契约：未测量的 RTO/RPO 字段（nil）在 JSON 中省略，
// 消费方以「字段缺失」区分未测量，而非把 0 误读为瞬时恢复/零数据丢失。
func TestResultJSONNilOmitted(t *testing.T) {
	b, err := json.Marshal(TestResult{TestID: "t1"})
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	for _, key := range []string{"rto_from_inject_sec", "rto_from_first_fail_sec", "rpo_time_sec"} {
		if strings.Contains(string(b), key) {
			t.Errorf("未测量字段 %s 应从 JSON 省略，实际输出: %s", key, b)
		}
	}
}

// TestResultJSONZeroPreserved 锁定契约另一半：真实测得 0 秒必须保留为 0，
// 不能被 omitempty 语义吞掉——这正是不能用裸 float64+omitempty 的原因。
func TestResultJSONZeroPreserved(t *testing.T) {
	zero := 0.0
	b, err := json.Marshal(TestResult{
		TestID:               "t1",
		RTO_FromFirstFailSec: &zero,
		RPOTimeSec:           &zero,
	})
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	for _, want := range []string{`"rto_from_first_fail_sec":0`, `"rpo_time_sec":0`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("真实 0 值应保留 %s，实际输出: %s", want, b)
		}
	}
}
