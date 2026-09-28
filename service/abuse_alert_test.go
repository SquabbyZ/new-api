package service

import (
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/stretchr/testify/assert"
)

// TestAbuseAlertFindingLineCannotCarryMarkup 是 S3 的回归断言：告警推送正文里
// **用户可控**的三个字段（令牌名 / 用户名 / 模型名）被注入 HTML 片段后，构造出来的
// 那一行不能再被当作标记解释。
//
// 为什么这条断言能失败：修复前正文直接拼接 `finding.TokenName` 等原值，而这条正文经
// NotifyRootUser → sendEmailNotify → common.SendEmail 发出，Content-Type 是
// **text/html**（common/email.go）。令牌名只校验长度（controller/token.go：
// `len(token.Name) > 50`），用户名同理，模型名来自客户端请求的 model —— 于是一个普通
// 账号就能往 root 的告警邮件里塞一段 HTML（伪造文案 + 外链）。
//
// 断言的对象是**正文构造点本身**（abuseAlertFindingLine），它是这条推送唯一的正文
// 构造点、只有一个调用方；所以它转义了，三个投递通道（email / webhook / Bark）拿到的
// 就是同一份已转义的正文。
func TestAbuseAlertFindingLineCannotCarryMarkup(t *testing.T) {
	// 「伪造文案 + 外链」是收件人真会被骗着点下去的形态，比 <script> 更贴近该场景：
	// 邮件客户端普遍不执行脚本，但一定会渲染链接。
	const injection = `<a href="https://evil.example/reset">Quota exceeded, re-authorise</a>`
	injectedModel := `gpt-4o<img src=x onerror=alert(1)>`

	line := abuseAlertFindingLine(AbuseAlertFindingPayload{
		Rule:      operation_setting.AbuseRuleConsumeSpike,
		Severity:  operation_setting.AbuseSeverityHigh,
		TokenID:   9001,
		TokenName: injection,
		UserID:    42,
		Username:  injection,
		ModelName: &injectedModel,
	})

	assert.NotContains(t, line, injection,
		"S3 FAILED [assertion: the injected fragment must not survive verbatim in the push body]: "+
			"a user-controlled token/user/model name reached the notification text unescaped")
	assert.False(t, strings.ContainsAny(line, `<>"'`),
		"S3 FAILED [assertion: after escaping, the push body must contain no HTML metacharacter, "+
			"so interpolating it into a text/html body cannot change the document structure]: line=%q", line)

	// 正控制：不含元字符的名字必须**逐字节不变** —— 转义不能把正常名字改坏，
	// 否则为了修一个注入会引入一个新的展示缺陷。
	benign := abuseAlertFindingLine(AbuseAlertFindingPayload{
		Rule:      operation_setting.AbuseRuleConsumeSpike,
		Severity:  operation_setting.AbuseSeverityHigh,
		TokenID:   9002,
		TokenName: "prod-key",
		UserID:    42,
		Username:  "customer-a",
		ModelName: nil,
	})
	assert.Contains(t, benign, "token prod-key of user customer-a",
		"S3 control FAILED [assertion: a name made of ordinary characters must pass through unchanged]")
}
