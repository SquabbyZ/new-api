package controller

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/gin-gonic/gin"
)

// abuseScanHandler 把异常用量扫描挂进既有的计划系统任务框架。
//
// 它与既有 4 个 handler 完全同形：租约去重、master-only 执行、运行历史、ctx 取消
// 全部由框架提供 —— 本 slice **不自建第二套去重**（那会与框架的 SystemTaskLock 冲突）。
//
// Enabled() 返回总开关：关掉时调度器不建行，于是运行历史里也不会出现扫描记录，
// 页面上的「上次扫描时间」因此仍然是诚实的（「根本没扫」是一种可区分的状态）。
type abuseScanHandler struct{}

func (abuseScanHandler) Type() string { return model.SystemTaskTypeAbuseScan }

func (abuseScanHandler) Enabled() bool { return operation_setting.IsAbuseAlertEnabled() }

func (abuseScanHandler) Interval() time.Duration {
	return time.Duration(operation_setting.AbuseAlertScanIntervalMinutes()) * time.Minute
}

func (abuseScanHandler) NewPayload() any { return nil }

func (abuseScanHandler) Run(ctx context.Context, task *model.SystemTask, runnerID string) {
	report, notified, err := service.RunAbuseAlertScan(ctx, time.Now())
	if err != nil {
		common.SysLog("abuse alert scan failed: " + err.Error())
		finishSystemTaskHandler(task, runnerID, model.SystemTaskStatusFailed, nil, err)
		return
	}
	finishSystemTaskHandler(task, runnerID, model.SystemTaskStatusSucceeded,
		service.SummarizeAbuseAlertReport(report, notified), nil)
}

// GetAbuseAlerts 是只读的「看清」端点。
//
// 它调用与定时任务**完全相同的检测函数**（service.BuildAbuseAlertReport →
// model.ScanAbuseAlerts），所以 HTTP 就能触发一次检测，不必等调度器 tick。
// GET **无副作用**：不写任何状态、不发通知。
//
// hours 超上限时钳制后正常返回（**不**报错、**不**扫描更大范围），因此一个 HTTP
// 请求不可能触发全表扫描。
func GetAbuseAlerts(c *gin.Context) {
	hours := 0
	if raw := c.Query("hours"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			// 负数 / 非数字一律回落到默认值，不返回 500。
			parsed = 0
		}
		hours = parsed
	}

	report, err := service.BuildAbuseAlertReport(c.Request.Context(), time.Now(), hours)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": "Failed to scan the log database: " + err.Error(),
			"data":    nil,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "",
		"data":    report,
	})
}
