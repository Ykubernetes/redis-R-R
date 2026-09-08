package app

import (
	"fmt"
	"sort"
	"time"
)

// printBanner 打印启动横幅与关键配置
func printBanner() {
	fmt.Println(`
  ____  _____ ____ ___ ____        ____ _____ ___        ____  ____   ___  
 |  _ \| ____|  _ \_ _/ ___|      |  _ \_   _/ _ \      |  _ \|  _ \ / _ \ 
 | |_) |  _| | | | | |\___ \ _____| |_) || || | | |_____| |_) | |_) | | | |
 |  _ <| |___| |_| | | ___) |_____|  _ < | || |_| |_____|  _ <|  __/| |_| |
 |_| \_\_____|____/___|____/      |_| \_\|_| \___/      |_| \_\_|    \___/`)
	fmt.Printf("Proxy Addr     : %s\n", cfg.Addr)
	fmt.Printf("Scan Method    : %s\n", cfg.ScanMethod)
	fmt.Printf("FailThreshold  : %d | RecoverN: %d | ProbeKeys: %d\n",
		cfg.FailThreshold, cfg.RecoverSuccessN, cfg.ProbeKeyCount)
	fmt.Printf("OpTimeout      : %s | 故障判定最坏阻塞 ≈ %s（fail-threshold × op-timeout）\n",
		cfg.OpTimeout, time.Duration(cfg.FailThreshold)*cfg.OpTimeout)
	if cfg.OpTimeout > 10*cfg.ProbeInterval {
		fmt.Printf("[WARN] op-timeout (%s) 超过 probe-interval (%s) 的 10 倍：故障期间单次探测阻塞将主导判定精度，追求精确时刻请调小 --op-timeout\n",
			cfg.OpTimeout, cfg.ProbeInterval)
	}
	fmt.Println("--------------------------------------------------------------")
	fmt.Println("提示：在进行POC测试前请确认Proxy的SCAN是否支持全局扫描，否则RPO可能不准")
	fmt.Println("--------------------------------------------------------------")
}

// printReport 打印完整测试报告
func printReport(r TestResult) {
	fmt.Println("\n╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                     测试报告                               ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Println("[测试标识]")
	fmt.Printf("TestID               : %s\n", r.TestID)
	fmt.Println()

	fmt.Println("[时间线]")
	fmt.Printf("用户标记故障时间     : %s\n", formatTime(r.FaultInjectTime))
	fmt.Printf("自动检测首次失败时间 : %s\n", formatTime(r.FirstFailTime))
	fmt.Printf("业务恢复判定时间     : %s\n", formatTime(r.RecoverTime))
	fmt.Println()

	fmt.Println("[RTO]")
	if r.RTO_FromFirstFailSec != nil {
		if *r.RTO_FromFirstFailSec < 0 {
			fmt.Printf("RTO           : %.3f 秒（异常：恢复时间早于首次失败时间）\n", *r.RTO_FromFirstFailSec)
		} else {
			fmt.Printf("RTO           : %.3f 秒\n", *r.RTO_FromFirstFailSec)
		}
	} else if r.RecoverTime.IsZero() && !r.FirstFailTime.IsZero() {
		fmt.Println("RTO           : 未能计算（测试结束时业务仍未恢复，RTO 超出最大运行时间）")
	} else if r.RecoverTime.IsZero() {
		fmt.Println("RTO           : 未能计算（未检测到故障或业务始终未恢复）")
	} else {
		fmt.Println("RTO           : 未能计算（缺少首次失败时间）")
	}
	if r.RTO_FromInjectSec != nil {
		if *r.RTO_FromInjectSec < 0 {
			fmt.Printf("RTO           : %.3f 秒（异常：恢复时间早于故障注入时间）\n", *r.RTO_FromInjectSec)
		} else {
			fmt.Printf("RTO           : %.3f 秒\n", *r.RTO_FromInjectSec)
		}
	} else if r.RecoverTime.IsZero() && !r.FaultInjectTime.IsZero() {
		fmt.Println("RTO           : 未能计算（测试结束时业务仍未恢复，RTO 超出最大运行时间）")
	} else if r.FaultInjectTime.IsZero() {
		fmt.Println("RTO           : 未能计算（用户未标记故障注入时间）")
	} else {
		fmt.Println("RTO           : 未能计算（缺少恢复时间）")
	}
	fmt.Println()

	fmt.Println("[RPO]")
	fmt.Printf("扫描方法             : %s\n", r.ScanMethodUsed)
	fmt.Printf("故障前最后成功序号   : %d（快照）\n", r.SeqAtFault)
	fmt.Printf("存活最大序号(≤快照)  : %d\n", r.MaxRecoveredSeq)
	fmt.Printf("运行结束最后成功序号 : %d（含恢复后写入，仅作对照）\n", r.LastSuccessSeq)
	if r.PostRecoverInvalidations > 0 {
		fmt.Printf("[警告] 恢复判定在观察期内被撤销 %d 次（瞬时抖动恢复），最终恢复时刻已按稳定恢复计\n", r.PostRecoverInvalidations)
	}
	if !r.RPOValid {
		fmt.Printf("RPO 有效性           : 无效 — %s\n", r.RPOInvalidReason)
		fmt.Println("估算丢失操作数       : （未计算）")
	} else {
		fmt.Println("RPO 有效性           : 有效")
		fmt.Printf("估算丢失操作数       : %d\n", r.LostOps)
		if r.SeqAtFault > 0 && r.MaxRecoveredSeq <= 0 {
			fmt.Println("[警告] 故障窗口内存活 seq key 为零：可能全部丢失，也可能扫描不完整，请结合扫描日志判断")
		}
		if r.RPOTimeSec != nil && *r.RPOTimeSec > 0 {
			fmt.Printf("RPO 时间窗口         : %.3f 秒\n", *r.RPOTimeSec)
		} else {
			fmt.Println("RPO 时间窗口         : ≈0 或无法精确计算")
		}
	}
	fmt.Println()

	fmt.Printf("总写入: %d | 成功: %d | 失败: %d | 探针失败: %d\n",
		r.TotalWrites, r.SuccessWrites, r.FailedWrites, r.ProbeFailCount)

	if len(r.ErrorStats) > 0 {
		fmt.Println("\n[错误分类]")
		keys := make([]string, 0, len(r.ErrorStats))
		for k := range r.ErrorStats {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if r.ErrorStats[keys[i]] != r.ErrorStats[keys[j]] {
				return r.ErrorStats[keys[i]] > r.ErrorStats[keys[j]]
			}
			return keys[i] < keys[j]
		})
		for _, k := range keys {
			fmt.Printf("%-20s : %d\n", k, r.ErrorStats[k])
			for _, s := range r.ErrorSamples[k] {
				fmt.Printf(" %s  ×%d  (%s)\n", s.Text, s.Count, s.Type)
			}
		}
	}
	fmt.Println()
}
