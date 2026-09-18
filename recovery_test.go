package searchengine

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReopenPersistsAfterFlush(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	ctx := context.Background()
	x, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := x.Put(ctx, Document{
			ID:     fmt.Sprintf("doc%d", i),
			Fields: []Field{{Name: "body", Value: fmt.Sprintf("hello world number %d", i), Store: true, Index: true}},
		}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := x.Delete(ctx, "doc2"); err != nil {
		t.Fatalf("del: %v", err)
	}
	if err := x.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	y, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer y.Close()
	res, err := y.Search(ctx, "hello", 0, 100)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if res.Total != 4 {
		t.Fatalf("want 4 live docs, got %d", res.Total)
	}
	for _, h := range res.Hits {
		if h.ID == "doc2" {
			t.Fatal("deleted doc reappeared after reopen")
		}
	}
}

func TestReopenReplaysAfterClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	ctx := context.Background()
	x, _ := Open(dir, nil)
	if err := x.Put(ctx, Document{ID: "a", Fields: []Field{
		{Name: "body", Value: "unflushed committed data", Store: true, Index: true},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
	y, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer y.Close()
	res, _ := y.Search(ctx, "unflushed", 0, 10)
	if res.Total != 1 {
		t.Fatalf("committed-then-close data lost: %+v", res)
	}
}

// TestSubprocessHardKillRecovery：子进程写入并打印 READY 后保持运行，
// 父进程 kill -9（SIGKILL）强杀，随后重开索引校验已确认写入仍可见。
func TestSubprocessHardKillRecovery(t *testing.T) {
	if os.Getenv("SE_CHILD") == "1" {
		runChildWriter()
		return
	}
	dir := filepath.Join(t.TempDir(), "idx")

	// 第一轮：写入并确认 READY，然后优雅退出（Close），建立已确认基线。
	if _, err := runChildAndWait(t, dir, 10, false); err != nil {
		t.Fatalf("child round1: %v", err)
	}

	// 第二轮：写入后由父进程在 READY 后立即强杀。
	cmd, err := startChild(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	waitReady(t, cmd)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = cmd.Wait()

	y, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen after kill -9: %v", err)
	}
	defer y.Close()
	res, err := y.Search(context.Background(), "crash", 0, 100)
	if err != nil {
		t.Fatalf("search after recovery: %v", err)
	}
	if res.Total < 10 {
		t.Fatalf("confirmed writes lost after kill -9: only %d visible", res.Total)
	}
	for _, h := range res.Hits {
		ok := false
		for _, sf := range h.Stored {
			if sf.Name == "body" && strings.Contains(sf.Value, "crash recovery doc") {
				ok = true
			}
		}
		if !ok {
			t.Fatalf("hit %s has corrupt/partial stored fields after recovery", h.ID)
		}
	}
}

var (
	childSeq  int64
	childRead = map[*exec.Cmd]io.Reader{}
)

func startChild(dir string, n int) (*exec.Cmd, error) {
	seq := atomic.AddInt64(&childSeq, 1)
	cmd := exec.Command(os.Args[0], "-test.run=TestSubprocessHardKillRecovery", "-test.v")
	cmd.Env = append(os.Environ(),
		"SE_CHILD=1",
		"SE_DIR="+dir,
		fmt.Sprintf("SE_N=%d", n),
		fmt.Sprintf("SE_SEQ=%d", seq),
	)
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	childRead[cmd] = r
	return cmd, nil
}

func waitReady(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	r := bufio.NewReader(childRead[cmd])
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		line, err := r.ReadString('\n')
		if strings.Contains(line, "READY") {
			return
		}
		if err != nil {
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				t.Fatalf("child exited before READY: %s", line)
			}
			return
		}
	}
	t.Fatal("timed out waiting for child READY")
}

func runChildAndWait(t *testing.T, dir string, n int, kill bool) (string, error) {
	cmd, err := startChild(dir, n)
	if err != nil {
		return "", err
	}
	waitReady(t, cmd)
	if kill {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	} else {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}
	return "", nil
}

func runChildWriter() {
	dir := os.Getenv("SE_DIR")
	var n, seq int
	fmt.Sscanf(os.Getenv("SE_N"), "%d", &n)
	fmt.Sscanf(os.Getenv("SE_SEQ"), "%d", &seq)
	x, err := Open(dir, nil)
	if err != nil {
		fmt.Println("CHILD_OPEN_ERROR:", err)
		os.Exit(2)
	}
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%d-%d", seq, i)
		err := x.Put(ctx, Document{ID: id, Fields: []Field{
			{Name: "body", Value: fmt.Sprintf("crash recovery doc %d round %d", i, seq), Store: true, Index: true},
		}})
		if err != nil {
			fmt.Println("CHILD_PUT_ERROR:", err)
			os.Exit(3)
		}
	}
	fmt.Println("READY")
	os.Stdout.Sync()
	// 优雅退出：收到 SIGINT 时关闭并退出；否则保持存活等待 kill。
	// SIGINT 默认会终止进程（Go test 对 SIGINT 直接退出），因此轮询一个标记文件。
	done := make(chan struct{})
	go func() {
		_ = x.Close()
		close(done)
	}()
	c := make(chan os.Signal, 1)
	signalNotify(c)
	select {
	case <-c:
		<-done
		os.Exit(0)
	case <-time.After(2 * time.Minute):
		os.Exit(4)
	}
}
