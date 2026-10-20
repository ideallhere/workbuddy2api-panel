package scheduler

import (
	"os"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/notify"
)

// 手动运行：WB_WEBHOOK_URL=... go test -run TestSendLive ./internal/scheduler/ -v
func TestSendLive(t *testing.T) {
	url := os.Getenv("WB_WEBHOOK_URL")
	if url == "" {
		t.Skip("set WB_WEBHOOK_URL to run")
	}
	n := notify.New(notify.Config{WebhookURL: url, Format: "wecom"})
	if n == nil {
		t.Fatal("nil sender")
	}
	// 同步验证 HTTP 通
	if err := n.SendTest(); err != nil {
		t.Fatalf("SendTest failed: %v", err)
	}
	// 再走 Send 路径（异步 goroutine）
	n.Send("task_result", "live-test-"+time.Now().Format("150405"), "🧪 Send 路径测试", "如果收到此条，Send 通路正常")
	time.Sleep(3 * time.Second)
}
