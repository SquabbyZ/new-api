package model

import (
	"fmt"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// quotaDataBusinessKeyIndex 是 8 列业务键的唯一索引名。模型 tag、迁移里的
// Migrator().CreateIndex 与测试断言三处必须完全一致，否则既有库上会出现两个索引。
const quotaDataBusinessKeyIndex = "idx_qdt_business_key"

// quotaDataBusinessKeyColumns 是 quota_data 的业务键，按顺序等于
// quotaDataBusinessKeyIndex 的列顺序，也是落库 upsert 的冲突目标与去重迁移的分组键。
var quotaDataBusinessKeyColumns = []string{
	"user_id",
	"username",
	"model_name",
	"created_at",
	"use_group",
	"token_id",
	"channel_id",
	"node_name",
}

// QuotaData 柱状图数据
type QuotaData struct {
	Id        int    `json:"id"`
	UserID    int    `json:"user_id" gorm:"index;uniqueIndex:idx_qdt_business_key,priority:1"`
	Username  string `json:"username" gorm:"index:idx_qdt_model_user_name,priority:2;uniqueIndex:idx_qdt_business_key,priority:2;size:64;default:''"`
	ModelName string `json:"model_name" gorm:"index:idx_qdt_model_user_name,priority:1;uniqueIndex:idx_qdt_business_key,priority:3;size:64;default:''"`
	CreatedAt int64  `json:"created_at" gorm:"bigint;index:idx_qdt_created_at,priority:2;uniqueIndex:idx_qdt_business_key,priority:4"`
	UseGroup  string `json:"use_group" gorm:"index;uniqueIndex:idx_qdt_business_key,priority:5;size:64;default:''"`
	TokenID   int    `json:"token_id" gorm:"index;uniqueIndex:idx_qdt_business_key,priority:6;default:0"`
	ChannelID int    `json:"channel_id" gorm:"index;uniqueIndex:idx_qdt_business_key,priority:7;default:0"`
	NodeName  string `json:"node_name" gorm:"index;uniqueIndex:idx_qdt_business_key,priority:8;size:64;default:''"`
	TokenUsed int    `json:"token_used" gorm:"default:0"`
	Count     int    `json:"count" gorm:"default:0"`
	Quota     int    `json:"quota" gorm:"default:0"`
}

type QuotaDataLogParams struct {
	UserID    int
	Username  string
	ModelName string
	Quota     int
	CreatedAt int64
	TokenUsed int
	UseGroup  string
	TokenID   int
	ChannelID int
	NodeName  string
}

func UpdateQuotaData() {
	for {
		if common.DataExportEnabled {
			common.SysLog("正在更新数据看板数据...")
			SaveQuotaDataCache()
		}
		time.Sleep(time.Duration(common.DataExportInterval) * time.Minute)
	}
}

var CacheQuotaData = make(map[string]*QuotaData)
var CacheQuotaDataLock = sync.Mutex{}

func logQuotaDataCache(quotaData *QuotaData) {
	key := fmt.Sprintf("%d\x00%s\x00%s\x00%d\x00%s\x00%d\x00%d\x00%s",
		quotaData.UserID,
		quotaData.Username,
		quotaData.ModelName,
		quotaData.CreatedAt,
		quotaData.UseGroup,
		quotaData.TokenID,
		quotaData.ChannelID,
		quotaData.NodeName,
	)
	count := quotaData.Count
	quota := quotaData.Quota
	tokenUsed := quotaData.TokenUsed
	cachedQuotaData, ok := CacheQuotaData[key]
	if ok {
		cachedQuotaData.Count += count
		cachedQuotaData.Quota += quota
		cachedQuotaData.TokenUsed += tokenUsed
		quotaData = cachedQuotaData
	}
	CacheQuotaData[key] = quotaData
}

func LogQuotaData(params QuotaDataLogParams) {
	// 只精确到小时
	createdAt := params.CreatedAt - (params.CreatedAt % 3600)
	quotaData := &QuotaData{
		UserID:    params.UserID,
		Username:  params.Username,
		ModelName: params.ModelName,
		CreatedAt: createdAt,
		UseGroup:  params.UseGroup,
		TokenID:   params.TokenID,
		ChannelID: params.ChannelID,
		NodeName:  params.NodeName,
		Count:     1,
		Quota:     params.Quota,
		TokenUsed: params.TokenUsed,
	}

	CacheQuotaDataLock.Lock()
	defer CacheQuotaDataLock.Unlock()
	logQuotaDataCache(quotaData)
}

func SaveQuotaDataCache() {
	CacheQuotaDataLock.Lock()
	defer CacheQuotaDataLock.Unlock()
	size := len(CacheQuotaData)
	// 缓存里已经按业务键求和过的值通过一条 upsert 落库：同键无行时插入，有行时按
	// 表限定列相加。单条语句不再有「先探测、都读到无行、再都插入」的交错窗口，
	// 配合数据库上的唯一索引，同一业务键最多只有一行。
	conflictColumns := make([]clause.Column, 0, len(quotaDataBusinessKeyColumns))
	for _, name := range quotaDataBusinessKeyColumns {
		conflictColumns = append(conflictColumns, clause.Column{Name: name})
	}
	failed := 0
	for key, quotaData := range CacheQuotaData {
		err := DB.Table("quota_data").Clauses(clause.OnConflict{
			Columns: conflictColumns,
			DoUpdates: clause.Assignments(map[string]any{
				"count":      gorm.Expr("quota_data.count + ?", quotaData.Count),
				"quota":      gorm.Expr("quota_data.quota + ?", quotaData.Quota),
				"token_used": gorm.Expr("quota_data.token_used + ?", quotaData.TokenUsed),
			}),
		}).Create(quotaData).Error
		// 写入失败的键保留在缓存中，等待下次刷写；只有成功落库的键才移除，
		// 否则这一轮累积的看板数据会随缓存清空一起丢失。
		if err != nil {
			failed++
			common.SysError(fmt.Sprintf("保存数据看板数据失败，该条数据保留待下次刷写: user_id=%d model_name=%s created_at=%d err=%s",
				quotaData.UserID, quotaData.ModelName, quotaData.CreatedAt, err))
			continue
		}
		delete(CacheQuotaData, key)
	}
	if failed == 0 {
		common.SysLog(fmt.Sprintf("保存数据看板数据成功，共保存%d条数据", size))
	}
}

func GetQuotaDataByUsername(username string, startTime int64, endTime int64) (quotaData []*QuotaData, err error) {
	var quotaDatas []*QuotaData
	// 从quota_data表中查询数据
	err = DB.Table("quota_data").
		Select("user_id, username, model_name, created_at, sum(count) as count, sum(quota) as quota, sum(token_used) as token_used").
		Where("username = ? and created_at >= ? and created_at <= ?", username, startTime, endTime).
		Group("user_id, username, model_name, created_at").
		Find(&quotaDatas).Error
	return quotaDatas, err
}

func GetQuotaDataByUserId(userId int, startTime int64, endTime int64) (quotaData []*QuotaData, err error) {
	var quotaDatas []*QuotaData
	// 从quota_data表中查询数据
	err = DB.Table("quota_data").
		Select("user_id, username, model_name, created_at, sum(count) as count, sum(quota) as quota, sum(token_used) as token_used").
		Where("user_id = ? and created_at >= ? and created_at <= ?", userId, startTime, endTime).
		Group("user_id, username, model_name, created_at").
		Find(&quotaDatas).Error
	return quotaDatas, err
}

func GetQuotaDataGroupByUser(startTime int64, endTime int64) (quotaData []*QuotaData, err error) {
	var quotaDatas []*QuotaData
	err = DB.Table("quota_data").
		Select("username, created_at, sum(count) as count, sum(quota) as quota, sum(token_used) as token_used").
		Where("created_at >= ? and created_at <= ?", startTime, endTime).
		Group("username, created_at").
		Find(&quotaDatas).Error
	return quotaDatas, err
}

func GetAllQuotaDates(startTime int64, endTime int64, username string) (quotaData []*QuotaData, err error) {
	if username != "" {
		return GetQuotaDataByUsername(username, startTime, endTime)
	}
	var quotaDatas []*QuotaData
	// 从quota_data表中查询数据
	// only select model_name, sum(count) as count, sum(quota) as quota, model_name, created_at from quota_data group by model_name, created_at;
	//err = DB.Table("quota_data").Where("created_at >= ? and created_at <= ?", startTime, endTime).Find(&quotaDatas).Error
	err = DB.Table("quota_data").Select("model_name, sum(count) as count, sum(quota) as quota, sum(token_used) as token_used, created_at").Where("created_at >= ? and created_at <= ?", startTime, endTime).Group("model_name, created_at").Find(&quotaDatas).Error
	return quotaDatas, err
}
