package app

import (
    "bufio"
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "sync"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "redis-rto-rpo",
    Short: "测量云Redis集群主从切换场景下的RTO与RPO",
    Long: `redis-rto-rpo 用于量化云Redis集群版-Proxy接入在主节点故障切换时的
业务中断时长(RTO)与数据丢失量(RPO)为容灾演练与SLA评估提供实测数据

工作原理：
  写入器以固定间隔持续写入带序号的key
  探针器以固定间隔写入探针key,连续失败达到阈值即判定故障开始
  连续成功达到阈值即判定业务恢复,测试结束后扫描存活的seq key
  以客户端最后成功序号 - 存活最大序号估算数据丢失量(RPO)

指标口径：
  RTO（推荐）：首次失败时刻 -> 业务恢复判定时刻,反映真实业务不可用时长
  RTO（参考）：用户标记的故障注入时刻 -> 业务恢复判定时刻
  RPO        ：丢失的写操作数量，及其对应的时间窗口（秒）

使用流程：
  1. 启动工具，等待「持续负载已启动」提示
  2. 通过云控制台或 API 触发主节点重启 / 故障切换
  3. 触发后立即回到终端按【回车】标记故障注入时刻 T0
  4. 工具自动检测故障开始与恢复，并在恢复后生成测试报告

注意：
  RPO计算依赖Proxy的SCAN/KEYS能返回全部分片的key，否则RPO会被高估;
  启动时的SCAN完整性检查即用于验证此点，集群场景请勿使用--skip-scan-check`,
    Example: `  # 连接本地Redis,使用默认参数
  redis-rto-rpo --addr 127.0.0.1:6379

  # 连接云Redis集群(密码格式为 实例ID:密码),并将结果保存为 JSON
  redis-rto-rpo --addr 10.141.111.2:6379 --password 'crs-xxxx:yourpass' --output-json result.json

  # 追求更高测量精度并指定test-id,便于区分多次测试
  redis-rto-rpo --addr 10.141.111.2:6379 --write-interval 10ms --probe-interval 50ms --op-timeout 200ms --test-id failover-01`,
    Run: runTest,
}

// Execute 运行根命令，失败则以非零码退出
func Execute() {
    if err := rootCmd.Execute(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func init() {
    rootCmd.Flags().StringVar(&cfg.Addr, "addr", "127.0.0.1:6379", "Redis 连接地址，格式为 host:port，云集群填 Proxy 的 VIP 地址")
    rootCmd.Flags().StringVar(&cfg.Password, "password", "", "连接密码，云Redis默认账号格式为 实例ID:密码；留空时读取环境变量 REDIS_PASSWORD（推荐，避免明文入 shell history）")
    rootCmd.Flags().StringVar(&cfg.KeyPrefix, "prefix", "rto_rpo_test", "测试key的统一前缀，用于隔离测试数据与清理")
    rootCmd.Flags().DurationVar(&cfg.WriteInterval, "write-interval", 20*time.Millisecond, "写入器的写入间隔，越小则RPO采样精度越高")
    rootCmd.Flags().DurationVar(&cfg.ProbeInterval, "probe-interval", 80*time.Millisecond, "探针器的探测间隔，越小则故障时刻判定越精确")
    rootCmd.Flags().DurationVar(&cfg.OpTimeout, "op-timeout", 500*time.Millisecond, "单次探测/写入命令的超时上限，是故障时刻判定的实际精度边界；超过 probe-interval 的 10 倍时启动横幅会给出精度告警")
    rootCmd.Flags().IntVar(&cfg.RecoverSuccessN, "recover-n", 6, "连续探测成功多少次判定业务已恢复，用于去抖")
    rootCmd.Flags().IntVar(&cfg.FailThreshold, "fail-threshold", 3, "连续探测失败多少次判定故障开始，用于去抖")
    rootCmd.Flags().IntVar(&cfg.ProbeKeyCount, "probe-keys", 6, "探针key数量，集群下建议设为分片数以便覆盖所有分片")
    rootCmd.Flags().DurationVar(&cfg.MaxRunTime, "max-run", 30*time.Minute, "最长运行时间，超时后自动停止并生成报告")
    rootCmd.Flags().DurationVar(&cfg.PostRecoverObserve, "post-recover-observe", 4*time.Second, "判定恢复后继续观察的时长，确保恢复状态稳定")
    rootCmd.Flags().IntVar(&cfg.PoolSize, "pool-size", 16, "连接池大小，测量场景为串行探针，默认值已足够")
    rootCmd.Flags().BoolVar(&cfg.CleanBeforeStart, "clean", false, "启动前清理历史遗留的测试 key（默认关闭，开启会删除 prefix:* 全部数据）")
    rootCmd.Flags().DurationVar(&cfg.StatusInterval, "status-interval", 3*time.Second, "运行期间实时状态的打印间隔")
    rootCmd.Flags().StringVar(&cfg.OutputJSON, "output-json", "", "将测试结果额外保存为JSON文件的路径，留空则不保存")
    rootCmd.Flags().StringVar(&cfg.ScanMethod, "scan-method", "scan", "扫描方式：scan用游标渐进扫描(推荐)，keys一次性返回（集群或大key量慎用）")
    rootCmd.Flags().BoolVar(&cfg.SkipScanCheck, "skip-scan-check", false, "跳过启动时的SCAN完整性检查（集群场景不建议，会导致RPO不可靠）")
    rootCmd.Flags().StringVar(&cfg.TestID, "test-id", "", "本次测试的唯一标识，用于隔离命名空间；默认自动生成毫秒级时间戳")
}

// runTest 执行 RTO/RPO 测量主流程
func runTest(cmd *cobra.Command, args []string) {
    if cfg.Password == "" {
        cfg.Password = os.Getenv("REDIS_PASSWORD")
    }
    // 测试唯一标识：默认用毫秒精度时间戳，避免同一秒内多次启动撞命名空间
    if cfg.TestID == "" {
        cfg.TestID = time.Now().Format("20060102150405.000")
    }
    if err := cfg.Validate(); err != nil {
        log.Fatalf("配置校验失败: %v", err)
    }

    printBanner()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigCh
        // 恢复默认信号行为：否则首个信号后 Notify 仍拦截 SIGINT，收尾期间
        // （wg.Wait + 扫描最长数分钟）用户再按 Ctrl+C 会毫无反应
        signal.Stop(sigCh)
        fmt.Println("\n收到退出信号，正在生成报告...（再次 Ctrl+C 强制退出）")
        cancel()
    }()

    writeClient := newClient()
    probeClient := newClient()
    defer func() {
        if err := writeClient.Close(); err != nil {
            log.Printf("关闭 write client 失败: %v", err)
        }
        if err := probeClient.Close(); err != nil {
            log.Printf("关闭 probe client 失败: %v", err)
        }
    }()

    if err := writeClient.Ping(ctx).Err(); err != nil {
        log.Fatalf("Write Client 连接失败: %v", err)
    }
    if err := probeClient.Ping(ctx).Err(); err != nil {
        log.Fatalf("Probe Client 连接失败: %v", err)
    }
    fmt.Println("[OK] Client 连接成功 Proxy代理模式")

    // 启动时 SCAN 完整性检查
    if !cfg.SkipScanCheck {
        fmt.Println("正在进行SCAN完整性检查 ...")
        if err := checkScanIntegrity(ctx, writeClient); err != nil {
            log.Fatalf("SCAN 完整性检查失败: %v\n建议：确认云Proxy的SCAN行为，或使用 --scan-method=keys 尝试", err)
        }
        fmt.Println("[OK] SCAN 完整性检查通过")
    } else {
        fmt.Println("[WARN] 已跳过 SCAN 完整性检查，RPO 结果可能不准确")
    }

    // 清理
    if cfg.CleanBeforeStart {
        fmt.Println("【警告】即将删除匹配", cfg.KeyPrefix+":*", "的 key")
        fmt.Println("3 秒后开始清理（可 Ctrl+C 取消）...")
        select {
        case <-time.After(3 * time.Second):
            deleted, failed, err := cleanTestKeys(ctx, writeClient)
            if err != nil {
                log.Fatalf("清理失败（扫描 key 出错，可能一个都没删）: %v", err)
            }
            if failed > 0 {
                fmt.Printf("[WARN] 清理完成但有 %d 个 key 删除失败（残留 key 属旧 TestID，TTL 到期自动清理，不影响本次测量）\n", failed)
            }
            fmt.Printf("[OK] 清理完成：删除 %d 个 key\n", deleted)
        case <-ctx.Done():
            fmt.Println("清理已取消")
            return
        }
    }

    // 用 hasKeys（强制 SCAN）而非 scanKeys：避免 --scan-method=keys 时在大实例上阻塞 Redis。
    stale, err := hasKeys(ctx, writeClient, cfg.KeyPrefix+":"+cfg.TestID+":seq:*")
    if err != nil {
        log.Fatalf("检查遗留 seq key 失败: %v", err)
    }
    if stale {
        log.Fatalf("test-id %q 下已存在遗留 seq key（上一轮未清理），会污染本轮 RPO 计算。请更换 --test-id 或加 --clean", cfg.TestID)
    }

    // 探针 key
    probeKeysList := make([]string, cfg.ProbeKeyCount)
    fmt.Println("探针 Key：")
    for i := range cfg.ProbeKeyCount {
        probeKeysList[i] = fmt.Sprintf("%s:%s:health:{%d}", cfg.KeyPrefix, cfg.TestID, i)
        fmt.Printf("  %s\n", probeKeysList[i])
    }

    // 共享状态：时间戳与去抖计数收敛在 probeState，writer 侧仅保留写入统计
    var (
        state probeState

        lastSuccessSeq atomic.Int64
        lastSuccessTS  atomic.Int64

        errorCollector = newErrorCollector()

        totalWriteAttempts atomic.Int64
        successWriteCount  atomic.Int64
    )

    // 用于停止写入和探针协程的信号
    stopCh := make(chan struct{})
    var wg sync.WaitGroup

    // 启动写入协程
    wg.Go(func() {
        startWriter(ctx, stopCh, writeClient, &totalWriteAttempts, &successWriteCount, &lastSuccessSeq, &lastSuccessTS, errorCollector)
    })

    // 启动探针协程
    wg.Go(func() {
        startProbe(ctx, stopCh, probeClient, probeKeysList, &state, &lastSuccessSeq, &lastSuccessTS, errorCollector)
    })

    // 实时状态
    wg.Go(func() {
        startStatusReporter(ctx, stopCh, &totalWriteAttempts, &successWriteCount, &state, &lastSuccessSeq, cfg.StatusInterval)
    })

    fmt.Println("\n>>> 持续负载已启动")
    fmt.Println(">>> 请通过云控制台/API 重启后端主节点")
    fmt.Println(">>> 重启操作完成后，立刻按【回车】标记 T0")

    go func() {
        // Scanln 在非交互 stdin（</dev/null、管道 EOF、CI）下立即返回错误；忽略它
        if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
            return
        }
        t0 := time.Now()
        state.mu.Lock()
        // ctx 已取消（SIGINT/超时进入收尾）时丢弃 T0：此刻标记已无测量意义
        if state.reportDone || ctx.Err() != nil {
            state.mu.Unlock()
            return
        }
        if state.faultTime.IsZero() {
            state.faultTime = t0
            fmt.Printf("\n[MANUAL] 用户标记故障注入时间 T0 = %s\n", t0.Format("15:04:05.000"))
        }
        state.mu.Unlock()
    }()

    timeout := time.After(cfg.MaxRunTime)
    checkTicker := time.NewTicker(500 * time.Millisecond)
    defer checkTicker.Stop()

    for {
        select {
        case <-ctx.Done():
            goto FINISH
        case <-timeout:
            fmt.Println("达到最大运行时间，停止")
            goto FINISH
        case <-checkTicker.C:
            if !state.isRecovered() {
                continue
            }
            // 观察窗口内若发生撤销（即使随后又重新判定恢复），说明恢复在抖动，
            for {
                invalBefore := state.invalidations.Load()
                select {
                case <-time.After(cfg.PostRecoverObserve):
                case <-ctx.Done():
                }
                stillRecovered := state.isRecovered()
                invalAfter := state.invalidations.Load()
                if ctx.Err() != nil {
                    goto FINISH
                }
                switch {
                case !stillRecovered:
                    fmt.Println("[OBSERVE] 恢复后观察期内判定被撤销，继续运行等待稳定恢复")
                case invalAfter > invalBefore:
                    fmt.Printf("[OBSERVE] 观察期内发生 %d 次撤销后重新恢复（抖动），重开 %s 观察窗口\n",
                        invalAfter-invalBefore, cfg.PostRecoverObserve)
                default:
                    goto FINISH
                }
                if !state.isRecovered() {
                    break
                }
            }
        }
    }

FINISH:
    // 禁止报告生成后 T0 标记 goroutine 再写入 faultTime（语义上无效的修改）
    state.mu.Lock()
    state.reportDone = true
    state.mu.Unlock()

    // 停止写入和探针协程，等待它们退出；wg.Wait 返回时在途命令均已完结，无需再 sleep
    close(stopCh)
    wg.Wait()

    // 创建新的上下文用于扫描，避免使用已取消的 ctx；超时按写入量动态放大
    scanCtx, scanCancel := context.WithTimeout(context.Background(), scanTimeout(totalWriteAttempts.Load()))
    defer scanCancel()

    // RPO 以故障确认时冻结的快照为上界：恢复后的写入不属于故障窗口，不得计入存活
    seqAtFault, tsAtFault := state.seqAtFault.Load(), state.tsAtFault.Load()
    fmt.Println("\n正在扫描计算 RPO（方法:", cfg.ScanMethod, "，上界 seq:", seqAtFault, ")...")
    maxSeq, maxTS, scanErr := scanMaxSeq(scanCtx, writeClient, seqAtFault)
    if scanErr != nil {
        fmt.Printf("[ERROR] 扫描失败，RPO 无效: %v\n", scanErr)
    } else {
        fmt.Printf("[OK] 存活最大 seq=%d（≤ 快照 %d）\n", maxSeq, seqAtFault)
    }

    state.mu.Lock()
    ft, fft, rt := state.faultTime, state.firstFailTime, state.recoverTime
    state.mu.Unlock()

    lost, rpoSec, rpoValid, rpoReason := computeRPO(seqAtFault, tsAtFault, maxSeq, maxTS, scanErr)

    result := TestResult{
        TestID:                   cfg.TestID,
        FaultInjectTime:          ft,
        FirstFailTime:            fft,
        RecoverTime:              rt,
        LastSuccessSeq:           lastSuccessSeq.Load(),
        LastSuccessTS:            lastSuccessTS.Load(),
        SeqAtFault:               seqAtFault,
        TSAtFault:                tsAtFault,
        MaxRecoveredSeq:          maxSeq,
        MaxRecoveredTS:           maxTS,
        LostOps:                  lost,
        RPOTimeSec:               rpoSec,
        RPOValid:                 rpoValid,
        RPOInvalidReason:         rpoReason,
        PostRecoverInvalidations: state.invalidations.Load(),
        ErrorStats:               errorCollector.Stats(),
        ErrorSamples:             errorCollector.Samples(),
        TotalWrites:              totalWriteAttempts.Load(),
        SuccessWrites:            successWriteCount.Load(),
        FailedWrites:             totalWriteAttempts.Load() - successWriteCount.Load(),
        ProbeFailCount:           state.probeFailCount.Load(),
        ScanMethodUsed:           cfg.ScanMethod,
    }

    if !rt.IsZero() {
        if !ft.IsZero() {
            result.RTO_FromInjectSec = new(rt.Sub(ft).Seconds())
        }
        if !fft.IsZero() {
            result.RTO_FromFirstFailSec = new(rt.Sub(fft).Seconds())
        }
    }

    printReport(result)

    if cfg.OutputJSON != "" {
        data, err := json.MarshalIndent(result, "", "  ")
        if err != nil {
            log.Printf("序列化结果 JSON 失败: %v", err)
            return
        }
        if err := os.WriteFile(cfg.OutputJSON, data, 0644); err != nil {
            log.Printf("写入结果文件 %s 失败: %v", cfg.OutputJSON, err)
            return
        }
        fmt.Printf("[OK] 结果已保存到: %s\n", cfg.OutputJSON)
    }
}

// 实时状态报告
func startStatusReporter(ctx context.Context, stopCh <-chan struct{}, totalWriteAttempts, successWriteCount *atomic.Int64,
    state *probeState, lastSeq *atomic.Int64, interval time.Duration) {

    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-stopCh:
            return
        case <-ticker.C:
            wa := totalWriteAttempts.Load()
            ws := successWriteCount.Load()
            pa := state.totalProbeAttempts.Load()
            ps := state.successProbeCount.Load()

            writeRate, probeRate := 0.0, 0.0
            if wa > 0 {
                writeRate = float64(ws) / float64(wa) * 100
            }
            if pa > 0 {
                probeRate = float64(ps) / float64(pa) * 100
            }

            cFail, cOK, recovered := state.counters()
            if recovered {
                cOK = cfg.RecoverSuccessN
            }

            fmt.Printf("[STATUS] write_ok=%.1f%% (%d/%d)  probe_ok=%.1f%% (%d/%d)  consec_fail=%d  consec_ok=%d  last_seq=%d\n",
                writeRate, ws, wa, probeRate, ps, pa, cFail, cOK, lastSeq.Load())
        }
    }
}
