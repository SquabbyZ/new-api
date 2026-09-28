package dto

type Notify struct {
	Type    string `json:"type"`
	Title   string `json:"title"`
	Content string `json:"content"`
	Values  []any  `json:"values"`
}

const ContentValueParam = "{{value}}"

const (
	NotifyTypeQuotaExceed   = "quota_exceed"
	NotifyTypeChannelUpdate = "channel_update"
	NotifyTypeChannelTest   = "channel_test"
	// NotifyTypeAbuseAlert 是异常用量告警的独立通知类型。它不能复用
	// channel_update：`type` 同时是通知限流桶的键（service/notify-limit.go 的
	// notify_limit:<user>:<type>:...）与 webhook 载荷的 type，复用会让滥用告警与
	// 渠道告警抢同一个通知预算，并让外部 webhook 无法区分两类事件。
	NotifyTypeAbuseAlert = "abuse_alert"
)

func NewNotify(t string, title string, content string, values []any) Notify {
	return Notify{
		Type:    t,
		Title:   title,
		Content: content,
		Values:  values,
	}
}
