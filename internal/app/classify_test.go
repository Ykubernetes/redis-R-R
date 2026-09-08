package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestClassifyErrorProtocol 验证 RESP 协议错误分类，逐一对应 go-redis
// parseTypedRedisError 的全部分派分支。
// go-redis 的 Is*Error 有三层判定：具体类型 -> 包装的 proto.RedisError 前缀 -> 文本前缀。
// 此处以文本构造，命中第三层；真实类型（第一层）由 TestClassifyErrorRealProtoType 覆盖。
// 注意：通用兜底桶 proto_error 与 nil_key 只认 redis.Error 接口、无文本 fallback，
// 故不在此用裸文本测试——真实链路中 go-redis 必产出实现该接口的类型，裸文本场景不存在。
func TestClassifyErrorProtocol(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		{"LOADING Redis is loading the dataset in memory", "loading"},
		{"READONLY You can't write against a read only replica.", "readonly"},
		{"MOVED 3999 127.0.0.1:6380", "moved"},
		{"ASK 3999 127.0.0.1:6381", "ask"},
		{"CLUSTERDOWN The cluster is down", "clusterdown"},
		{"NOREPLICAS Not enough good replicas to write", "noreplicas"},
		// 以下为主从切换期高发、此前全部误落 other 的错误
		{"MASTERDOWN Link with MASTER is down", "masterdown"},
		{"TRYAGAIN Multiple keys request during rehashing of slot", "tryagain"},
		{"ERR max number of clients reached", "maxclients"},
		{"NOAUTH Authentication required.", "auth"},
		{"WRONGPASS invalid username-password pair", "auth"},
		{"NOPERM this user has no permissions to run the command", "noperm"},
		{"EXECABORT Transaction discarded because of previous errors.", "execabort"},
		{"OOM command not allowed when used memory > 'maxmemory'.", "oom"},
	}
	for _, c := range cases {
		got := classifyError(errors.New(c.text))
		if got != c.want {
			t.Errorf("%q: 期望 %q，实际 %q", c.text, c.want, got)
		} else {
			t.Logf("%s -> %s ✓", c.text, got)
		}
	}
}

// TestClassifyErrorRealProtoType 用 go-redis 导出的真实 proto.RedisError 常量验证：
// 真实链路里 RESP 错误的具体 Go 类型（而非裸文本）同样能被正确归类，且 errors.As
// 可穿透任意包装。这是「文本模拟」之外的第一层判定路径。
func TestClassifyErrorRealProtoType(t *testing.T) {
	var re redis.Error
	if !errors.As(redis.ErrCrossSlot, &re) {
		t.Fatalf("前置条件失败：redis.ErrCrossSlot 未实现 redis.Error，本用例无意义")
	}

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"CROSSSLOT", redis.ErrCrossSlot, "proto_error"},
		{"NOSCRIPT", redis.ErrNoScript, "proto_error"},
		{"CROSSSLOT被包装", fmt.Errorf("set: %w", redis.ErrCrossSlot), "proto_error"},
		{"nil键", redis.Nil, "nil_key"},
		{"nil键被包装", fmt.Errorf("get: %w", redis.Nil), "nil_key"},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Errorf("%s: 期望 %q，实际 %q（类型=%T）", c.name, c.want, got, c.err)
		} else {
			t.Logf("%s: %T -> %s ✓", c.name, c.err, got)
		}
	}
}

// TestClassifyErrorWrapped 验证被包装的错误仍能正确归类（errors.Is/As 穿透包装）。
func TestClassifyErrorWrapped(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"池耗尽", redis.ErrPoolTimeout, "pool_timeout"},
		{"池耗尽被包装", fmt.Errorf("get conn: %w", redis.ErrPoolTimeout), "pool_timeout"},
		{"ctx取消被包装", fmt.Errorf("set: %w", context.Canceled), "ctx_canceled"},
		{"ctx超时被包装", fmt.Errorf("set: %w", context.DeadlineExceeded), "ctx_deadline"},
		{"客户端已关闭", fmt.Errorf("set: %w", redis.ErrClosed), "conn_closed"},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Errorf("%s: 期望 %q，实际 %q", c.name, c.want, got)
		} else {
			t.Logf("%s -> %s ✓", c.name, got)
		}
	}
}

func TestClassifyErrorStd(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"ctx取消", context.Canceled, "ctx_canceled"},
		{"ctx超时", context.DeadlineExceeded, "ctx_deadline"},
		{"文本兜底timeout", fmt.Errorf("i/o timeout"), "timeout"},
		{"文本兜底refused", fmt.Errorf("dial tcp: connection refused"), "connection_refused"},
		{"未知错误", fmt.Errorf("something weird"), "other"},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Errorf("%s: 期望 %q，实际 %q", c.name, c.want, got)
		} else {
			t.Logf("%s -> %s ✓", c.name, got)
		}
	}

	if got := classifyError(nil); got != "" {
		t.Errorf("nil 错误不应产生分类 key，实际 %q", got)
	}
}

// TestClassifyErrorRealDisconnect 验证连接层错误的归类。
// 注：实测腾讯云 Proxy 倾向回 RESP 协议错误而非断 TCP，故这些分支在真实
// failover 中可能一次都不命中；保留是因为其它 Proxy/直连场景确实会产出它们。
// go-redis 在 TCP 断连时真实产出的是 *net.OpError（包裹 syscall.Errno 或超时），
// 故重点验证 net.OpError 路径，os.SyscallError 仅作等价对照。
func TestClassifyErrorRealDisconnect(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		// go-redis 真实产出：net.OpError 包裹 syscall.Errno
		{"OpError读重置", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, "connection_reset"},
		{"OpError写管道破裂", &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}, "broken_pipe"},
		{"OpError超时", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, "timeout"},
		{"OpError拒连", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, "connection_refused"},
		{"对端重置(reset)", os.NewSyscallError("read", syscall.ECONNRESET), "connection_reset"},
		{"管道破裂(pipe)", os.NewSyscallError("write", syscall.EPIPE), "broken_pipe"},
		{"连接关闭(eof)", io.EOF, "eof"},
		{"连接关闭(unexpected eof)", io.ErrUnexpectedEOF, "eof"},
		{"reset被包装", fmt.Errorf("ping: %w", os.NewSyscallError("read", syscall.ECONNRESET)), "connection_reset"},
		{"OpError被包装", fmt.Errorf("set: %w", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}), "connection_reset"},
		{"标准库连接已关闭", net.ErrClosed, "conn_closed"},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Errorf("%s: 期望 %q，实际 %q（错误文本=%v）", c.name, c.want, got, c.err)
		} else {
			t.Logf("%s: %v -> %s ✓", c.name, c.err, got)
		}
	}
}

// TestClassifyErrorPriority 验证判定顺序：上层语义必须优先于下层，否则会被掩盖。
func TestClassifyErrorPriority(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		// 池耗尽文本含 "timeout"，若顺序颠倒会被误判为网络超时
		{"池耗尽优先于timeout文本", fmt.Errorf("redis: connection pool timeout"), "timeout"},
		{"真池耗尽优先于timeout文本", redis.ErrPoolTimeout, "pool_timeout"},
		// ctx 取消导致的失败不得被记成协议错误
		{"ctx取消优先于协议文本", fmt.Errorf("%w: CLUSTERDOWN", context.Canceled), "ctx_canceled"},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Errorf("%s: 期望 %q，实际 %q", c.name, c.want, got)
		} else {
			t.Logf("%s -> %s ✓", c.name, got)
		}
	}
}

func TestErrorCollectorRecord(t *testing.T) {
	c := newErrorCollector()
	if got := c.Stats(); len(got) != 0 {
		t.Errorf("新收集器应为空，实际 %v", got)
	}

	c.record(nil)
	if got := c.Stats(); len(got) != 0 {
		t.Errorf("nil 错误不应计数，实际 %v", got)
	}
	if got := c.Samples(); len(got) != 0 {
		t.Errorf("nil 错误不应留样本，实际 %v", got)
	}

	c.record(errors.New("MASTERDOWN Link with MASTER is down"))
	c.record(errors.New("MASTERDOWN Link with MASTER is down"))
	c.record(errors.New("CLUSTERDOWN The cluster is down"))

	stats := c.Stats()
	if stats["masterdown"] != 2 {
		t.Errorf("masterdown 期望 2，实际 %d", stats["masterdown"])
	}
	if stats["clusterdown"] != 1 {
		t.Errorf("clusterdown 期望 1，实际 %d", stats["clusterdown"])
	}

	samples := c.Samples()
	if len(samples["masterdown"]) != 1 {
		t.Fatalf("同形态错误应去重为 1 条样本，实际 %d 条: %+v", len(samples["masterdown"]), samples["masterdown"])
	}
	if samples["masterdown"][0].Count != 2 {
		t.Errorf("去重样本计数应为 2，实际 %d", samples["masterdown"][0].Count)
	}
	if samples["masterdown"][0].Type != "*errors.errorString" {
		t.Errorf("样本应记录真实 Go 类型，实际 %q", samples["masterdown"][0].Type)
	}
}

// TestErrorCollectorDedupByType 验证同文本但不同 Go 类型不被去重：
// 类型是区分「Proxy 回协议错误」与「客户端本地错误」的关键信息，合并会丢证据。
func TestErrorCollectorDedupByType(t *testing.T) {
	c := newErrorCollector()
	c.record(errors.New("i/o timeout"))
	c.record(fmt.Errorf("wrapped: %w", os.NewSyscallError("read", syscall.ETIMEDOUT)))

	samples := c.Samples()
	if len(samples["timeout"]) != 2 {
		t.Fatalf("不同 Go 类型应各留一条样本，实际 %d 条: %+v", len(samples["timeout"]), samples["timeout"])
	}
}

func TestErrorCollectorSampleCap(t *testing.T) {
	c := newErrorCollector()
	for i := range maxSamplesPerBucket + 3 {
		c.record(fmt.Errorf("unknown weird error variant %d", i))
	}

	stats := c.Stats()
	if stats["other"] != int64(maxSamplesPerBucket+3) {
		t.Errorf("计数应保留全部 %d 次，实际 %d", maxSamplesPerBucket+3, stats["other"])
	}
	samples := c.Samples()
	if len(samples["other"]) != maxSamplesPerBucket {
		t.Errorf("样本应截断为 %d 条，实际 %d 条", maxSamplesPerBucket, len(samples["other"]))
	}
}

// TestErrorCollectorTruncatesLongText 验证超长错误文本被截断且多字节字符不被切断。
func TestErrorCollectorTruncatesLongText(t *testing.T) {
	c := newErrorCollector()
	long := strings.Repeat("主从切换", maxSampleTextLen)
	c.record(errors.New(long))

	samples := c.Samples()
	got := samples["other"][0].Text
	if !strings.HasSuffix(got, "...") {
		t.Errorf("超长文本应以省略号结尾，实际结尾 %q", got[len(got)-10:])
	}
	if len([]rune(got)) != maxSampleTextLen+len("...") {
		t.Errorf("截断后应为 %d 个字符 + 省略号，实际 %d", maxSampleTextLen, len([]rune(got)))
	}
	if strings.Contains(got, "\uFFFD") {
		t.Error("截断不应产生非法多字节字符")
	}

	short := "MASTERDOWN Link with MASTER is down"
	c2 := newErrorCollector()
	c2.record(errors.New(short))
	if got := c2.Samples()["masterdown"][0].Text; got != short {
		t.Errorf("短文本不应被改写，期望 %q 实际 %q", short, got)
	}
}

// TestErrorCollectorSnapshotIsolation 验证返回的是快照，外部修改不污染内部状态。
func TestErrorCollectorSnapshotIsolation(t *testing.T) {
	c := newErrorCollector()
	c.record(errors.New("MASTERDOWN Link with MASTER is down"))

	stats := c.Stats()
	stats["masterdown"] = 999
	stats["injected"] = 1
	if got := c.Stats()["masterdown"]; got != 1 {
		t.Errorf("内部计数被外部快照污染：%d", got)
	}
	if _, ok := c.Stats()["injected"]; ok {
		t.Error("外部向快照写入的 key 泄漏进了内部状态")
	}

	samples := c.Samples()
	samples["masterdown"][0].Text = "tampered"
	if got := c.Samples()["masterdown"][0].Text; got == "tampered" {
		t.Error("内部样本被外部快照污染")
	}
}

// TestErrorCollectorConcurrent 验证 writer 与 probe 并发记录时无数据竞争。
// 必须以 go test -race 运行才能真正检出竞争。
func TestErrorCollectorConcurrent(t *testing.T) {
	c := newErrorCollector()
	errs := []error{
		errors.New("MASTERDOWN Link with MASTER is down"),
		errors.New("CLUSTERDOWN The cluster is down"),
		errors.New("TRYAGAIN Multiple keys request"),
		os.NewSyscallError("read", syscall.ECONNRESET),
		context.Canceled,
	}

	const goroutines, perGoroutine = 8, 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			for i := range perGoroutine {
				c.record(errs[(g+i)%len(errs)])
			}
		}()
	}
	wg.Wait()

	var total int64
	for _, v := range c.Stats() {
		total += v
	}
	if total != goroutines*perGoroutine {
		t.Errorf("期望累计 %d 次，实际 %d", goroutines*perGoroutine, total)
	}
}

func TestTruncateSampleText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"空串", "", ""},
		{"短于上限不变", "MASTERDOWN", "MASTERDOWN"},
		{"恰好等于上限不变", strings.Repeat("a", maxSampleTextLen), strings.Repeat("a", maxSampleTextLen)},
		{"超一字节即截断", strings.Repeat("a", maxSampleTextLen+1), strings.Repeat("a", maxSampleTextLen) + "..."},
		{"多字节按字符截断", strings.Repeat("切", maxSampleTextLen+1), strings.Repeat("切", maxSampleTextLen) + "..."},
	}
	for _, c := range cases {
		got := truncateSampleText(c.in)
		if got != c.want {
			t.Errorf("%s: 期望长度 %d，实际长度 %d", c.name, len([]rune(c.want)), len([]rune(got)))
		}
	}
}
