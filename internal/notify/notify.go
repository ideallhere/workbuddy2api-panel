// Package notify 事件推送：账号限流/封禁/余额阈值/积分过期/签到结果，经通用 Webhook 发出。
//
// 设计对齐本仓库既有范式：
//   - "失败仅打日志、不向上抛"（persist.go 落盘失败 notification 注释同款哲学）：
//     推送失败只记日志，绝不影响主流程（签到/选号/代理）。
//   - 防轰炸：同 key 事件在 SilenceWindow 内只发首条（对齐 pool cooldown 设计哲学）。
//   - 零破坏：Hook 模式（对齐 scheduler.GrowthHook）——sender 为 nil 时所有调用点直接跳过。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// 事件类型常量（面板配置里按名开关）。
const (
	EventAccountCooled   = "account_cooled"   // 账号进入冷却（429 等）
	EventAccountBanned   = "account_banned"   // 账号被封禁/禁用
	EventCreditLow       = "credit_low"       // 余额低于阈值
	EventCreditExpiring  = "credit_expiring"  // 积分即将过期
	EventCheckinResult   = "checkin_result"   // 签到结果（成功/失败）
	EventServiceAbnormal = "service_abnormal" // 服务异常（上游不可用等）
	EventTaskResult      = "task_result"      // 排程任务结果（签到/旅行/活跃/保活/夜猫子）
)

// Config 推送配置（config.json 的 notify 节，支持热更新）。
type Config struct {
	WebhookURL      string        `json:"webhook_url"`       // 通用 webhook 地址（企微机器人等）
	Format          string        `json:"format"`            // wecom | dingtalk | feishu | bark | raw，默认 wecom
	EnabledEvents   []string      `json:"enabled_events"`    // 启用的事件类型；空 = 全部启用
	CreditThreshold float64       `json:"credit_threshold"`  // credit_low 阈值（账号剩余积分）
	SilenceWindow   string        `json:"silence_window"`    // 同事件静默期，默认 "30m"
	Timeout         string        `json:"timeout"`           // 请求超时，默认 "10s"
}

// Sender 推送器。
type Sender struct {
	mu       sync.Mutex
	cfg      Config
	client   *http.Client
	silence  map[string]time.Time // 防轰炸：eventKey → 上次发送时间
	location *time.Location
}

// New 构造推送器。cfg.WebhookURL 为空时返回 nil（调用方按 nil 判空跳过）。
func New(cfg Config) *Sender {
	if strings.TrimSpace(cfg.WebhookURL) == "" {
		return nil
	}
	s := &Sender{
		cfg:      cfg,
		silence:  make(map[string]time.Time),
		location: time.Local,
	}
	d := cfg.Timeout
	if _, err := time.ParseDuration(d); err != nil || d == "" {
		d = "10s"
	}
	if to, err := time.ParseDuration(d); err == nil {
		s.client = &http.Client{Timeout: to}
	} else {
		s.client = &http.Client{Timeout: 10 * time.Second}
	}
	return s
}

// Reconfigure 热更新配置（面板保存配置时调用）。URL 清空则禁用。
func (s *Sender) Reconfigure(cfg Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	if strings.TrimSpace(cfg.WebhookURL) == "" {
		s.client = nil
	}
}

// Enabled 判断某事件是否启用（nil receiver = 未配置推送，一律 false）。
func (s *Sender) Enabled(event string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil || strings.TrimSpace(s.cfg.WebhookURL) == "" {
		return false
	}
	if len(s.cfg.EnabledEvents) == 0 {
		return true
	}
	for _, e := range s.cfg.EnabledEvents {
		if e == event {
			return true
		}
	}
	return false
}

// Send 推送事件。失败仅记日志。
//   - event: 事件类型常量
//   - key:   防轰炸去重键（如 "account_cooled:uid123"）；空 = 不去重
//   - title: 标题（企微 markdown 首行）
//   - lines: 明细行
func (s *Sender) Send(event, key, title string, lines ...string) {
	if !s.Enabled(event) {
		return
	}
	s.mu.Lock()
	// 防轰炸检查
	if key != "" {
		win := 30 * time.Minute
		if d, err := time.ParseDuration(s.cfg.SilenceWindow); err == nil && s.cfg.SilenceWindow != "" {
			win = d
		}
		if last, ok := s.silence[key]; ok && time.Since(last) < win {
			s.mu.Unlock()
			return
		}
		s.silence[key] = time.Now()
		// 简单清理：超过 1000 条时丢弃最旧的（面板常驻进程，事件键有界但保险起见）
		if len(s.silence) > 1000 {
			for k, t := range s.silence {
				if time.Since(t) > win {
					delete(s.silence, k)
				}
			}
		}
	}
	cfg := s.cfg
	client := s.client
	s.mu.Unlock()

	body := buildBody(cfg.Format, title, lines)
	if body == nil {
		log.Printf("[notify] unknown format %q, skip", cfg.Format)
		return
	}
	go func() {
		resp, err := client.Post(cfg.WebhookURL, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("[notify] send %s failed: %v", event, err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			log.Printf("[notify] send %s got HTTP %d", event, resp.StatusCode)
		}
	}()
}

// SendSync 同步推送（用于进程退出前等需要确保送达的场景）。失败仅记日志。
func (s *Sender) SendSync(event, key, title string, lines ...string) {
	if !s.Enabled(event) {
		return
	}
	body := buildBody(s.cfg.Format, title, lines)
	if body == nil {
		return
	}
	resp, err := s.client.Post(s.cfg.WebhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[notify] send %s failed: %v", event, err)
		return
	}
	resp.Body.Close()
}

// SendTest 发送一条测试消息（同步、绕过静默窗口），返回投递结果供面板直接提示。
func (s *Sender) SendTest() error {
	if s == nil {
		return fmt.Errorf("notify not configured")
	}
	s.mu.Lock()
	cfg := s.cfg
	client := s.client
	s.mu.Unlock()
	if client == nil || strings.TrimSpace(cfg.WebhookURL) == "" {
		return fmt.Errorf("webhook_url 为空")
	}
	body := buildBody(cfg.Format, "🧪 WorkBuddy2API 推送测试",
		[]string{"通知功能已启用", "来自面板手动测试"})
	resp, err := client.Post(cfg.WebhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	// 企微/钉钉/飞书对错误 key 也返回 200，body 里带 errcode——尽力解析提示
	var m struct {
		Errcode int    `json:"errcode"`
		Errmsg  string `json:"errmsg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err == nil && m.Errcode != 0 {
		return fmt.Errorf("errcode %d: %s", m.Errcode, m.Errmsg)
	}
	return nil
}

// buildBody 按渠道格式构造 JSON 请求体。
func buildBody(format, title string, lines []string) []byte {
	content := title
	if len(lines) > 0 {
		content += "\n" + strings.Join(lines, "\n")
	}
	content = fmt.Sprintf("%s\n%s", title, strings.Join(lines, "\n"))
	_ = content
	switch strings.ToLower(format) {
	case "dingtalk":
		return []byte(fmt.Sprintf(`{"msgtype":"markdown","markdown":{"title":%q,"text":%q}}`,
			title, strings.Join(lines, "\n\n")))
	case "feishu":
		return []byte(fmt.Sprintf(`{"msg_type":"text","content":{"text":%q}}`,
			title+"\n"+strings.Join(lines, "\n")))
	case "bark":
		// bark 走 URL path，调用方需把 webhook_url 配成 https://api.day.app/<key>/
		return []byte(fmt.Sprintf(`{"title":%q,"body":%q}`, title, strings.Join(lines, "\n")))
	case "raw":
		return []byte(fmt.Sprintf(`{"title":%q,"lines":%q}`, title, strings.Join(lines, "\n")))
	default: // wecom 企业微信机器人 markdown
		md := fmt.Sprintf("## %s\n%s", title, strings.Join(lines, "\n"))
		return []byte(fmt.Sprintf(`{"msgtype":"markdown","markdown":{"content":%q}}`, md))
	}
}

// CreditThreshold 返回余额阈值（<=0 = 未启用 credit_low 推送）。
func (s *Sender) CreditThreshold() float64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.CreditThreshold
}

// Now 便于测试注入时间。
var Now = time.Now
