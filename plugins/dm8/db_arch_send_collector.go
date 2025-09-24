package dm8

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// DbArchSendDetailInfo 归档发送详情信息
type DbArchSendDetailInfo struct {
	archDest sql.NullString
	archType sql.NullString
	lsnDiff  sql.NullFloat64
}

// DbArchSendCollector 归档发送监控采集器
type DbArchSendCollector struct {
	db                 *sql.DB
	archSendDetailInfo *prometheus.Desc // 归档发送详情
	archSendDiffValue  *prometheus.Desc // 归档发送差值

	// 每个实例独立的视图检查缓存
	archSendFieldsCheckOnce sync.Once
	archSendFieldsExist     bool
	archApplyInfoCheckOnce  sync.Once
	archApplyInfoExists     bool

	config *Config
}

// NewDbArchSendCollector 初始化归档发送监控采集器
func NewDbArchSendCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbArchSendCollector{
		db:     db,
		config: config,
		archSendDetailInfo: prometheus.NewDesc(
			dmdbms_arch_send_detail_info,
			"Information about DM database archive send detail info, return MAX_SEND_LSN - LAST_SEND_LSN = diffValue",
			[]string{"arch_type", "arch_dest"},
			nil,
		),
		archSendDiffValue: prometheus.NewDesc(
			dmdbms_arch_send_diff_value,
			"Information about DM database archive send diff value, return MAX_SEND_LSN - LAST_SEND_LSN = diffValue",
			[]string{"arch_type", "arch_dest"},
			nil,
		),
	}
}

func (c *DbArchSendCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.archSendDetailInfo
	ch <- c.archSendDiffValue
}

func (c *DbArchSendCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout*time.Second)
	defer cancel()

	// 快速检查归档是否开启
	if !c.isArchiveEnabled(context.Background()) {
		// 归档未开启时，不采集发送相关指标
		return
	}

	// 查询所有归档发送详情信息
	dbArchSendInfos, err := c.getDbArchSendDetailInfo(ctx, c.db)
	if err != nil {

		handleDbQueryError(err)
		return
	}

	for _, dbArchSendInfo := range dbArchSendInfos {
		archType := NullStringToString(dbArchSendInfo.archType)
		archDest := NullStringToString(dbArchSendInfo.archDest)
		lsnDiff := NullFloat64ToFloat64(dbArchSendInfo.lsnDiff)

		// 发送详情指标（优化后：仅保留arch_type和arch_dest标签）
		ch <- prometheus.MustNewConstMetric(
			c.archSendDetailInfo,
			prometheus.GaugeValue,
			lsnDiff,
			archType, archDest,
		)

		// LSN差值指标（简化版，用于监控延迟）
		ch <- prometheus.MustNewConstMetric(
			c.archSendDiffValue,
			prometheus.GaugeValue,
			lsnDiff,
			archType, archDest,
		)
	}
}

// isArchiveEnabled 快速检查归档是否开启
func (c *DbArchSendCollector) isArchiveEnabled(ctx context.Context) bool {
	var paraValue string
	query := `SELECT /*+DMDB_CHECK_FLAG*/ PARA_VALUE FROM v$dm_ini WHERE para_name='ARCH_INI'`
	err := c.db.QueryRowContext(ctx, query).Scan(&paraValue)
	if err != nil {
		logger.Infof("[%s] Failed to check archive status: %v", err)
		return false
	}

	if paraValue != "1" {
		return false
	}

	// 进一步检查归档状态是否VALID
	var archStatus string
	query = `SELECT /*+DMDB_CHECK_FLAG*/ CASE arch_status WHEN 'VALID' THEN '1' WHEN 'INVALID' THEN '0' END FROM v$arch_status WHERE arch_type='LOCAL'`
	err = c.db.QueryRowContext(ctx, query).Scan(&archStatus)
	if err != nil {
		logger.Infof("[%s] Failed to check archive validity: %v", err)
		return false
	}

	return archStatus == "1"
}

// checkArchSendInfoFields 检查V$ARCH_SEND_INFO视图中的特定字段是否存在
func (c *DbArchSendCollector) checkArchSendInfoFields(ctx context.Context) bool {
	c.archSendFieldsCheckOnce.Do(func() {
		var count int
		if err := c.db.QueryRowContext(ctx, QueryArchSendInfoFieldsExist).Scan(&count); err != nil {
			logger.Warnf("[checkArchSendInfoFields] Failed to check V$ARCH_SEND_INFO fields existence: %v", err)
			c.archSendFieldsExist = false
			return
		}
		// 如果count为2，说明两个字段都存在
		c.archSendFieldsExist = count == 2
		logger.Infof("[checkArchSendInfoFields] V$ARCH_SEND_INFO fields exist: %v (LAST_SEND_CODE，LAST_SEND_DESC)", c.archSendFieldsExist)
	})
	return c.archSendFieldsExist
}

// checkArchApplyInfoExists 检查V$ARCH_APPLY_INFO视图是否存在
func (c *DbArchSendCollector) checkArchApplyInfoExists(ctx context.Context) bool {
	c.archApplyInfoCheckOnce.Do(func() {
		var count int
		if err := c.db.QueryRowContext(ctx, QueryArchApplyInfoExists).Scan(&count); err != nil {
			logger.Warnf("[%s] V$ARCH_APPLY_INFO not accessible: %v", err)
			c.archApplyInfoExists = false
			return
		}
		c.archApplyInfoExists = count == 1
		logger.Infof("[checkArchApplyInfoExists] V$ARCH_APPLY_INFO exists: %v", c.archApplyInfoExists)
	})
	return c.archApplyInfoExists
}

// getDbArchSendDetailInfo 查询所有归档发送详情信息
func (c *DbArchSendCollector) getDbArchSendDetailInfo(ctx context.Context, db *sql.DB) ([]DbArchSendDetailInfo, error) {
	// 根据视图存在性选择合适的查询SQL
	var querySql string
	if c.checkArchApplyInfoExists(ctx) {
		querySql = QueryArchSendDetailInfo2
	} else {
		querySql = QueryArchSendDetailInfo
	}

	// 检查V$ARCH_SEND_INFO视图中的字段是否存在
	if !c.checkArchSendInfoFields(ctx) {
		// 如果字段不存在，将相关字段替换为空字符串
		querySql = strings.ReplaceAll(querySql, "LAST_SEND_CODE,", "'' AS LAST_SEND_CODE,")
		querySql = strings.ReplaceAll(querySql, "LAST_SEND_DESC,", "'' AS LAST_SEND_DESC,")
	}

	var dbArchSendDetailInfos []DbArchSendDetailInfo
	rows, err := db.QueryContext(ctx, querySql)
	if err != nil {
		logger.Errorf("[getDbArchSendDetailInfo] Failed to query dbArchSendDetailInfo: %v", err)
		return dbArchSendDetailInfos, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbArchSendDetailInfo DbArchSendDetailInfo
		// 跳过不需要的字段
		var lastSendCode, lastSendDesc, lastStartTime, lastEndTime, lastSendTime sql.NullString
		if err := rows.Scan(&dbArchSendDetailInfo.archDest, &dbArchSendDetailInfo.archType,
			&dbArchSendDetailInfo.lsnDiff, &lastSendCode,
			&lastSendDesc, &lastStartTime,
			&lastEndTime, &lastSendTime); err != nil {
			logger.Errorf(fmt.Sprintf("[getDbArchSendDetailInfo] Error scanning row"))
			continue
		}
		dbArchSendDetailInfos = append(dbArchSendDetailInfos, dbArchSendDetailInfo)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf(fmt.Sprintf("[getDbArchSendDetailInfo] Error with rows"))
	}

	return dbArchSendDetailInfos, nil
}
