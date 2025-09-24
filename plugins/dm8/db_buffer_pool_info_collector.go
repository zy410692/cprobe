package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// 定义数据结构
type BufferPoolInfo struct {
	bufferName sql.NullString
	hitRate    sql.NullFloat64
}

// 定义收集器结构体
type DbBufferPoolInfoCollector struct {
	db                 *sql.DB
	bufferPoolInfoDesc *prometheus.Desc
	config             *Config
}

func NewDbBufferPoolCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbBufferPoolInfoCollector{
		db:     db,
		config: config,
		bufferPoolInfoDesc: prometheus.NewDesc(
			dmdbms_bufferpool_info,
			"Information about DM database bufferpool return hitRate",
			[]string{"buffer_name"},
			nil,
		),
	}
}

func (c *DbBufferPoolInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bufferPoolInfoDesc
}

func (c *DbBufferPoolInfoCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QueryBufferPoolHitRateInfoSql)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryBufferPoolHitRateInfoSql, err)
		return
	}
	defer rows.Close()

	var bufferPoolInfos []BufferPoolInfo
	for rows.Next() {
		var info BufferPoolInfo
		if err := rows.Scan(&info.bufferName, &info.hitRate); err != nil {
			logger.Errorf("Error scanning row,err: %s", err)
			continue
		}
		bufferPoolInfos = append(bufferPoolInfos, info)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf(" Error with rows,err: %s", err)
	}

	// 发送数据到 Prometheus
	for _, info := range bufferPoolInfos {

		bufferName := NullStringToString(info.bufferName)
		hitRate := NullFloat64ToFloat64(info.hitRate)
		ch <- prometheus.MustNewConstMetric(
			c.bufferPoolInfoDesc,
			prometheus.GaugeValue,
			hitRate,
			bufferName,
		)
	}
}
