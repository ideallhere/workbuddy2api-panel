package notify

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildBodyWecom(t *testing.T) {
	b := buildBody("wecom", "标题", []string{"行1", "行2"})
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("wecom body invalid json: %v", err)
	}
	if m["msgtype"] != "markdown" {
		t.Fatalf("wecom msgtype = %v", m["msgtype"])
	}
}

func TestBuildBodyFeishu(t *testing.T) {
	b := buildBody("feishu", "标题", []string{"行1"})
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("feishu body invalid: %v", err)
	}
	if m["msg_type"] != "text" {
		t.Fatalf("feishu msg_type = %v", m["msg_type"])
	}
}

func TestSilenceWindow(t *testing.T) {
	s := New(Config{WebhookURL: "http://127.0.0.1:1/x", Format: "raw", SilenceWindow: "1s"})
	if s == nil {
		t.Fatal("sender should not be nil with url set")
	}
	if !s.Enabled(EventCreditLow) {
		t.Fatal("event should be enabled by default (empty enabled list)")
	}
	// 首发通过（会失败投递但会记 silence 键）
	s.Send(EventCreditLow, "k1", "t")
	s.mu.Lock()
	_, ok := s.silence["k1"]
	s.mu.Unlock()
	if !ok {
		t.Fatal("silence key should be recorded")
	}
}

func TestNilSenderSafe(t *testing.T) {
	var s *Sender
	if s.Enabled("x") {
		t.Fatal("nil sender must report disabled")
	}
	if s.CreditThreshold() != 0 {
		t.Fatal("nil sender threshold must be 0")
	}
	s.Send("x", "", "t") // 不应 panic
}

func TestEmptyURL(t *testing.T) {
	if New(Config{}) != nil {
		t.Fatal("empty webhook_url should yield nil sender")
	}
}

func TestReconfigureDisable(t *testing.T) {
	s := New(Config{WebhookURL: "http://127.0.0.1:1/x"})
	s.Reconfigure(Config{})
	time.Sleep(10 * time.Millisecond)
	if s.Enabled("x") {
		t.Fatal("sender should be disabled after clearing url")
	}
}
