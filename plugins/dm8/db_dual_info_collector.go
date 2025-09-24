package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DB_DUAL_FAILUR = 0
)

// 定义收集器结构体
type DbDualInfoCollector struct {
	db           *sql.DB
	dualInfoDesc *prometheus.Desc
	config       *Config
}

func NewDbDualCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbDualInfoCollector{
		db:     db,
		config: config,
		dualInfoDesc: prometheus.NewDesc(
			dmdbms_dual_info,
			"Information about DM database query dual table info,return false is 0, true is 1",
			[]string{},
			nil,
		),
	}
}

func (c *DbDualInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.dualInfoDesc
}

func (c *DbDualInfoCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	dualValue := c.QueryDualInfo(ctx)

	// 发送数据到 Prometheus
	ch <- prometheus.MustNewConstMetric(
		c.dualInfoDesc,
		prometheus.GaugeValue,
		dualValue,
	)

}

func (c *DbDualInfoCollector) QueryDualInfo(ctx context.Context) float64 {
	var dualValue float64
	rows, err := c.db.QueryContext(ctx, QueryDualInfoSql)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryDualInfoSql, err)
		return DB_DUAL_FAILUR
	}
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&dualValue)
	if err != nil {
		logger.Errorf("[QueryDualInfo] Error scanning dual value error:%s ", err)
		return DB_DUAL_FAILUR
	}

	return dualValue
}
