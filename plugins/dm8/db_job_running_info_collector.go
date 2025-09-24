package dm8

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

type DbJobRunningInfoCollector struct {
	db              *sql.DB
	jobErrorNumDesc *prometheus.Desc
	config          *Config
}

// 定义存储查询结果的结构体
type ErrorCountInfo struct {
	ErrorNum sql.NullInt64
}

func NewDbJobRunningInfoCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbJobRunningInfoCollector{
		db:     db,
		config: config,
		jobErrorNumDesc: prometheus.NewDesc(
			dmdbms_joblog_error_num,
			"dmdbms_joblog_error_num info information",
			[]string{"host_name"}, // 添加标签
			nil,
		),
	}
}

func (c *DbJobRunningInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobErrorNumDesc
}

func (c *DbJobRunningInfoCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QueryDbJobRunningInfoSqlStr)
	if err != nil {
		// 检查报错信息中是否包含 "v$dmmonitor" 字符串
		if strings.Contains(err.Error(), "SYSJOB") {
			logger.Warnf("[%s] 数据库未开启定时任务功能，无法检查错误任务异常数量。请执行sql语句call SP_INIT_JOB_SYS(1); 开启定时作业的功能。（该报错不影响其他指标采集数据,也可忽略）", QueryDbJobRunningInfoSqlStr)
			return
		}
		handleDbQueryErrorWithSQL(QueryDbJobRunningInfoSqlStr, err)
		return
	}
	defer rows.Close()

	// 存储查询结果
	var errorCountInfo ErrorCountInfo
	if rows.Next() {
		if err := rows.Scan(&errorCountInfo.ErrorNum); err != nil {
			logger.Errorf("Error scanning row has error: %s", err)
			return
		}
	}

	if err := rows.Err(); err != nil {
		logger.Errorf("[%s] Error with rows", err)
	}
	// 发送数据到 Prometheus

	ch <- prometheus.MustNewConstMetric(c.jobErrorNumDesc, prometheus.GaugeValue, NullInt64ToFloat64(errorCountInfo.ErrorNum), Hostname)

}
