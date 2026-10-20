package notify

import (
	"fmt"
	"testing"
)

func TestEnabledDebug(t *testing.T) {
	cfg := Config{WebhookURL: "https://example.com/hook", Format: "wecom", EnabledEvents: nil}
	s := New(cfg)
	fmt.Println("Enabled(task_result):", s.Enabled("task_result"))
	fmt.Println("Enabled(checkin_result):", s.Enabled("checkin_result"))
}
