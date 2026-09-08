package app

import "time"

// TestResult 单次测量结果，用于终端报告和 JSON 输出
type TestResult struct {
	TestID                   string                   `json:"test_id"`
	FaultInjectTime          time.Time                `json:"fault_inject_time"`
	FirstFailTime            time.Time                `json:"first_fail_time"`
	RecoverTime              time.Time                `json:"recover_time"`
	RTO_FromInjectSec        *float64                 `json:"rto_from_inject_sec,omitempty"`
	RTO_FromFirstFailSec     *float64                 `json:"rto_from_first_fail_sec,omitempty"`
	LastSuccessSeq           int64                    `json:"last_success_seq"`
	LastSuccessTS            int64                    `json:"last_success_ts"`   // Unix 毫秒
	SeqAtFault               int64                    `json:"seq_at_fault"`      // 故障确认时冻结的最后成功序号（RPO 基准）
	TSAtFault                int64                    `json:"ts_at_fault"`       // 快照对应的 Unix 毫秒
	MaxRecoveredSeq          int64                    `json:"max_recovered_seq"` // 存活最大序号（≤ seq_at_fault）
	MaxRecoveredTS           int64                    `json:"max_recovered_ts"`  // Unix 毫秒
	LostOps                  int64                    `json:"lost_ops"`
	RPOTimeSec               *float64                 `json:"rpo_time_sec,omitempty"`
	RPOValid                 bool                     `json:"rpo_valid"`
	RPOInvalidReason         string                   `json:"rpo_invalid_reason,omitempty"`
	PostRecoverInvalidations int64                    `json:"post_recover_invalidations"` // 恢复判定被撤销次数
	TotalWrites              int64                    `json:"total_writes"`
	SuccessWrites            int64                    `json:"success_writes"`
	FailedWrites             int64                    `json:"failed_writes"`
	ProbeFailCount           int64                    `json:"probe_fail_count"`
	ErrorStats               map[string]int64         `json:"error_stats"`
	ErrorSamples             map[string][]errorSample `json:"error_samples"`
	ScanMethodUsed           string                   `json:"scan_method_used"`
}
