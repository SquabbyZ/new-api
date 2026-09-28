package service

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/setting/operation_setting"
)

// AbuseAlertEvidence 是一条发现的证据。全部是相对该令牌自身历史的量 ——
// 绝对量在流量增长时会误报、在缓慢恶化时会沉默，所以判定本体永远是比率 + 方向。
type AbuseAlertEvidence struct {
	Current        float64 `json:"current"`
	BaselineMedian float64 `json:"baseline_median"`
	Ratio          float64 `json:"ratio"`
	Direction      string  `json:"direction"`
}

// AbuseAlertFindingPayload 是下发给页面的一条发现。
//
// 它**不含任何凭据**：没有 tokens.key、没有请求体、没有响应体、没有 IP
// （本 slice 本来也拿不到 IP）。TokenName / Username 为空表示令牌已被删除 ——
// 该行仍然显示，只是标成已删除，因为丢掉一条发现等于静默隐藏盗刷。
type AbuseAlertFindingPayload struct {
	Rule        string             `json:"rule"`
	Severity    string             `json:"severity"`
	TokenID     int32              `json:"token_id"`
	TokenName   string             `json:"token_name"`
	UserID      int                `json:"user_id"`
	Username    string             `json:"username"`
	ModelName   *string            `json:"model_name"`
	WindowStart int64              `json:"window_start"`
	WindowEnd   int64              `json:"window_end"`
	Evidence    AbuseAlertEvidence `json:"evidence"`
}

// AbuseAlertLogRetentionPayload 是 Q2 要求的日志保留期提示数据。
type AbuseAlertLogRetentionPayload struct {
	RetentionDays   int  `json:"retention_days"`
	RequiredMinutes int  `json:"required_minutes"`
	RequiredHours   int  `json:"required_hours"`
	Sufficient      bool `json:"sufficient"`
}

// AbuseAlertReport 是 GET /api/abuse-alerts/ 的响应体。
//
// PRD 冻结的字段（window_minutes / generated_at / baseline_ready / findings /
// skipped / excluded_saturated_rows）按字面保留，其余是**追加**字段，用来满足
// PRD 自己的边界 case 与 R5 的可区分性要求（少了它们，页面只能显示「没有通知」，
// 无法区分「没触发」「触发了但没发出去」「根本没扫」）。
//
// 没有 `notify_enabled`：总开关的通知意图已经随伪键 abuse_alert_setting.notify_enabled
// 下发（页面读的是那一个），端点再带一份没有任何消费者，属投机字段，已删。
type AbuseAlertReport struct {
	Hours                 int                           `json:"hours"`
	WindowMinutes         int                           `json:"window_minutes"`
	GeneratedAt           int64                         `json:"generated_at"`
	BaselineReady         bool                          `json:"baseline_ready"`
	BaselineReadyAt       int64                         `json:"baseline_ready_at"`
	LastScheduledScanAt   int64                         `json:"last_scheduled_scan_at"`
	NotifyChannelReady    bool                          `json:"notify_channel_ready"`
	LogRetention          AbuseAlertLogRetentionPayload `json:"log_retention"`
	Findings              []AbuseAlertFindingPayload    `json:"findings"`
	Skipped               []model.AbuseAlertSkip        `json:"skipped"`
	ExcludedSaturatedRows int64                         `json:"excluded_saturated_rows"`
}

// BuildAbuseAlertReport 跑一次检测并组装响应。**无副作用**：不写任何状态、不发通知。
//
// 它与定时任务调用的是同一个 model.ScanAbuseAlerts，因此 HTTP 就能触发检测，
// 不必等调度器 tick —— 这对可验证性是决定性的，也保证了不存在第二份检测逻辑。
func BuildAbuseAlertReport(ctx context.Context, now time.Time, hours int) (AbuseAlertReport, error) {
	result, err := model.ScanAbuseAlerts(ctx, now, hours)
	if err != nil {
		return AbuseAlertReport{}, err
	}

	report := AbuseAlertReport{
		Hours:                 clampAbuseAlertHours(hours),
		WindowMinutes:         result.WindowMinutes,
		GeneratedAt:           now.Unix(),
		BaselineReady:         result.BaselineReady,
		BaselineReadyAt:       result.BaselineReadyAt,
		LastScheduledScanAt:   model.AbuseAlertLastScheduledScanAt(),
		NotifyChannelReady:    rootNotificationChannelReady(),
		LogRetention:          abuseAlertLogRetention(),
		Skipped:               result.Skipped,
		ExcludedSaturatedRows: result.ExcludedSaturatedRows,
		Findings:              []AbuseAlertFindingPayload{},
	}

	tokenIDs := make([]int32, 0, len(result.Findings))
	for _, finding := range result.Findings {
		tokenIDs = append(tokenIDs, finding.TokenID)
	}
	owners := model.ResolveAbuseAlertOwners(tokenIDs)

	for _, finding := range result.Findings {
		owner := owners[finding.TokenID]
		report.Findings = append(report.Findings, AbuseAlertFindingPayload{
			Rule:        finding.Rule,
			Severity:    finding.Severity,
			TokenID:     finding.TokenID,
			TokenName:   owner.TokenName,
			UserID:      owner.UserID,
			Username:    owner.Username,
			ModelName:   finding.ModelName,
			WindowStart: finding.WindowStart,
			WindowEnd:   finding.WindowEnd,
			Evidence: AbuseAlertEvidence{
				Current:        finding.Current,
				BaselineMedian: finding.BaselineMedian,
				Ratio:          finding.Ratio,
				Direction:      finding.Direction,
			},
		})
	}
	return report, nil
}

// clampAbuseAlertHours 与 model 层的钳制保持一致，让响应里的 hours 就是实际用的值。
func clampAbuseAlertHours(hours int) int {
	if hours <= 0 {
		return operation_setting.AbuseAlertMaxEndpointHours()
	}
	return min(hours, operation_setting.AbuseAlertMaxEndpointHours())
}

func abuseAlertLogRetention() AbuseAlertLogRetentionPayload {
	retentionDays := model.ClickHouseLogTTLDays()
	requiredMinutes := operation_setting.AbuseAlertBaselineGateMinutes()
	requiredHours := operation_setting.AbuseAlertNewModelLookbackHours()
	// RetentionDays == 0 表示该日志库不设 TTL（SQLite / MySQL / PostgreSQL 走
	// 日志清理任务），此时「保留期不足」这个判断不成立，不提示。
	sufficient := true
	if retentionDays > 0 {
		sufficient = retentionDays*24*60 >= requiredMinutes && retentionDays >= requiredHours
	}
	return AbuseAlertLogRetentionPayload{
		RetentionDays:   retentionDays,
		RequiredMinutes: requiredMinutes,
		RequiredHours:   requiredHours,
		Sufficient:      sufficient,
	}
}

// rootNotificationChannelReady 报告 root 用户是否真的配了一条能收通知的通道。
//
// 这是 R5 的第三块拼图：NotifyUser 在 root 没有配通道时**直接返回 nil 而不报错**
// （service/user_notify.go 的空邮箱 / 空 webhook 分支），告警会静默消失。
// 页面必须能把「没有通知」区分成没触发 / 触发了但没发出去 / 根本没扫。
func rootNotificationChannelReady() bool {
	user := model.GetRootUser()
	setting := user.GetSetting()
	switch setting.NotifyType {
	case dto.NotifyTypeEmail, "":
		// NotifyUser 优先用 NotificationEmail，为空时回落到用户的默认邮箱。
		return setting.NotificationEmail != "" || user.Email != ""
	case dto.NotifyTypeWebhook:
		return setting.WebhookUrl != ""
	case dto.NotifyTypeBark:
		return setting.BarkUrl != ""
	case dto.NotifyTypeGotify:
		return setting.GotifyUrl != "" && setting.GotifyToken != ""
	default:
		return false
	}
}

// RunAbuseAlertScan 执行一次定时扫描：检测 + 推送高危发现。
//
// 「记录宽松、推送克制」：findings 包含全部命中，**只有 high 才推送**，且
// **一次扫描只推一条汇总**（NotifyLimitCount 默认 2，逐条推会把预算烧在一个令牌上）。
// 按需触发扫描的 root 用户不一定配了通道，所以页面是完整记录，推送只是余光。
func RunAbuseAlertScan(ctx context.Context, now time.Time) (AbuseAlertReport, bool, error) {
	report, err := BuildAbuseAlertReport(ctx, now, 0)
	if err != nil {
		return AbuseAlertReport{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return report, false, err
	}

	pushed := false
	if operation_setting.IsAbuseAlertNotifyEnabled() {
		pushed = notifyAbuseAlertFindings(report)
	}
	return report, pushed, nil
}

// notifyAbuseAlertFindings 把本次扫描的 high 发现汇总成一条通知推给 root。
// 返回是否真的尝试推送（没配置通道时仍会尝试，由 NotifyUser 自己按语义跳过并记日志）。
func notifyAbuseAlertFindings(report AbuseAlertReport) bool {
	high := make([]AbuseAlertFindingPayload, 0, len(report.Findings))
	for _, finding := range report.Findings {
		if finding.Severity == operation_setting.AbuseSeverityHigh {
			high = append(high, finding)
		}
	}
	if len(high) == 0 {
		return false
	}

	subject := fmt.Sprintf("Abuse alert: %d high-severity finding(s)", len(high))
	lines := make([]string, 0, len(high)+1)
	lines = append(lines, "A background scan found abnormal usage on the following tokens. This is an alert only — no token, user or group was changed.")
	for _, finding := range high {
		lines = append(lines, abuseAlertFindingLine(finding))
	}
	NotifyRootUser(dto.NotifyTypeAbuseAlert, subject, strings.Join(lines, "\n"))
	return true
}

// abuseAlertFindingLine 渲染一条发现。缺失的令牌/用户名显示成 deleted，
// 不隐藏该行，也不显示任何凭据。
//
// **用户可控字段必须在这里做 HTML 转义（S3）**：令牌名只校验长度
// （controller/token.go 的 `len(token.Name) > 50`），用户名同理，模型名来自客户端
// 请求的 model —— 三者都是普通账号可控的。而这条正文经 NotifyRootUser →
// sendEmailNotify → common.SendEmail 发出，Content-Type 是 **text/html**
// （common/email.go），于是未转义时任何用户都能往 root 的告警邮件里塞一段 HTML
// （伪造文案、外链、隐藏文字）。
//
// 修法选在这里、而不是改通知设施：`common.SendEmail` / `NotifyUser` 是所有通知
// 共用的，动它们会改到别的调用方的正文（例如给既有邮件加一层转义或换掉
// Content-Type）。本函数是这条推送**唯一**的正文构造点，且只有一个调用方
// （notifyAbuseAlertFindings），所以转义落在这里影响面为零。
//
// 代价如实记录：webhook / Bark / Gotify 收到的是同一份正文，名字里含
// `& < > " '` 时那些通道会看到 HTML 实体（不转义的名字逐字节不变）。
// 用一个字符串同时满足 HTML 与纯文本两个上下文是有取舍的，这里选的是
// 「宁可多一个实体，不可让用户文本被当成标记解释」。
func abuseAlertFindingLine(finding AbuseAlertFindingPayload) string {
	tokenName := html.EscapeString(finding.TokenName)
	if tokenName == "" {
		tokenName = fmt.Sprintf("#%d (deleted)", finding.TokenID)
	}
	username := html.EscapeString(finding.Username)
	if username == "" {
		username = fmt.Sprintf("#%d (deleted)", finding.UserID)
	}
	target := fmt.Sprintf("token %s of user %s", tokenName, username)
	if finding.ModelName != nil {
		target = fmt.Sprintf("%s (model %s)", target, html.EscapeString(*finding.ModelName))
	}
	return fmt.Sprintf(
		"- [%s] %s: %s=%.2f vs baseline median %.2f (ratio %.2f, %s), window %s",
		finding.Rule,
		target,
		finding.Rule,
		finding.Evidence.Current,
		finding.Evidence.BaselineMedian,
		finding.Evidence.Ratio,
		finding.Evidence.Direction,
		time.Unix(finding.WindowStart, 0).UTC().Format(time.RFC3339),
	)
}

// AbuseAlertScanSummary 是一次定时扫描落进系统任务运行历史的摘要。
type AbuseAlertScanSummary struct {
	Findings              int   `json:"findings"`
	HighFindings          int   `json:"high_findings"`
	Skipped               int   `json:"skipped"`
	ExcludedSaturatedRows int64 `json:"excluded_saturated_rows"`
	Notified              bool  `json:"notified"`
}

// SummarizeAbuseAlertReport 汇总一次扫描的结果供系统任务记录。
func SummarizeAbuseAlertReport(report AbuseAlertReport, notified bool) AbuseAlertScanSummary {
	summary := AbuseAlertScanSummary{
		Findings:              len(report.Findings),
		Skipped:               len(report.Skipped),
		ExcludedSaturatedRows: report.ExcludedSaturatedRows,
		Notified:              notified,
	}
	for _, finding := range report.Findings {
		if finding.Severity == operation_setting.AbuseSeverityHigh {
			summary.HighFindings++
		}
	}
	return summary
}
