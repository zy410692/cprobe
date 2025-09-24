package dm8

import (
	"context"
	"database/sql"
	"time"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// DbArchSwitchRateInfo 归档切换频率信息
type DbArchSwitchRateInfo struct {
	minusDiff sql.NullFloat64
}

// DbArchSwitchCollector 归档切换监控采集器
type DbArchSwitchCollector struct {
	db                       *sql.DB
	archSwitchRateDesc       *prometheus.Desc // 归档切换频率
	archSwitchRateDetailInfo *prometheus.Desc // 归档切换频率详情
	config                   *Config
}

// NewDbArchSwitchCollector 初始化归档切换监控采集器
func NewDbArchSwitchCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbArchSwitchCollector{
		db:     db,
		config: config,
		archSwitchRateDesc: prometheus.NewDesc(
			dmdbms_arch_switch_rate,
			"Information about DM database archive switch rate，Always output the most recent piece of data",
			[]string{},
			nil,
		),
		archSwitchRateDetailInfo: prometheus.NewDesc(
			dmdbms_arch_switch_rate_detail_info,
			"Information about DM database archive switch rate info, return MAX_SEND_LSN - LAST_SEND_LSN = diffValue",
			[]string{},
			nil,
		),
	}
}

func (c *DbArchSwitchCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.archSwitchRateDesc
	ch <- c.archSwitchRateDetailInfo
}

func (c *DbArchSwitchCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout*time.Second)
	defer cancel()

	// 快速检查归档是否开启
	if !c.isArchiveEnabled(ctx) {
		// 归档未开启时返回默认值0
		ch <- prometheus.MustNewConstMetric(
			c.archSwitchRateDesc,
			prometheus.GaugeValue,
			0,
		)
		return
	}

	// 查询归档切换频率
	dbArchSwitchRateInfo, err := c.getDbArchSwitchRate(ctx, c.db)
	if err != nil {
		handleDbQueryError(err)
		return
	}

	minusDiff := NullFloat64ToFloat64(dbArchSwitchRateInfo.minusDiff)

	// 归档切换频率指标（用于折线图）
	ch <- prometheus.MustNewConstMetric(
		c.archSwitchRateDesc,
		prometheus.GaugeValue,
		minusDiff,
	)

	// 归档切换详细信息（优化后：不包含额外标签）
	ch <- prometheus.MustNewConstMetric(
		c.archSwitchRateDetailInfo,
		prometheus.GaugeValue,
		minusDiff,
	)
}

// isArchiveEnabled 快速检查归档是否开启
func (c *DbArchSwitchCollector) isArchiveEnabled(ctx context.Context) bool {
	var paraValue string
	query := `SELECT /*+DMDB_CHECK_FLAG*/ PARA_VALUE FROM v$dm_ini WHERE para_name='ARCH_INI'`
	err := c.db.QueryRowContext(context.Background(), query).Scan(&paraValue)
	if err != nil {
		logger.Infof("[isArchiveEnabled] Failed to check archive status: %v,SQL: %s", err, query)
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
		logger.Infof("[isArchiveEnabled] Failed to check archive validity: %v", err)
		return false
	}

	return archStatus == "1"
}

// getDbArchSwitchRate 查询归档切换频率
func (c *DbArchSwitchCollector) getDbArchSwitchRate(ctx context.Context, db *sql.DB) (DbArchSwitchRateInfo, error) {
	var dbArchSwitchRateInfo DbArchSwitchRateInfo

	rows, err := db.QueryContext(ctx, QueryArchiveSwitchRateSql)
	if err != nil {
		logger.Errorf("[getDbArchSwitchRate] Failed to query archive switch rate: %v,SQL: %s", err, QueryArchiveSwitchRateSql)
		return dbArchSwitchRateInfo, err
	}
	defer rows.Close()

	if rows.Next() {
		// 跳过不需要的字段，只扫描最后的 minusDiff
		var status, createTime, path, clsn, srcDbMagic sql.NullString
		if err := rows.Scan(&status, &createTime, &path, &clsn, &srcDbMagic, &dbArchSwitchRateInfo.minusDiff); err != nil {
			logger.Errorf("[%s] Error scanning row", err)
			return dbArchSwitchRateInfo, err
		}
	}

	return dbArchSwitchRateInfo, nil
}
