package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

type PurgeCollector struct {
	dbPool       *sql.DB
	purgeObjects *prometheus.Desc
	config       *Config
}

// PurgeInfo 存储回滚段信息
type PurgeInfo struct {
	ObjNum int64
}

func NewPurgeCollector(dbPool *sql.DB, config *Config) MetricCollector {
	return &PurgeCollector{
		dbPool: dbPool,
		config: config,
		purgeObjects: prometheus.NewDesc(
			dmdbms_purge_objects_info,
			"Number of purge objects",
			[]string{"host_name"},
			nil,
		),
	}
}

func (c *PurgeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.purgeObjects
}

func (c *PurgeCollector) Collect(ch chan<- prometheus.Metric) {

	// 获取回滚段数据
	purgeInfos, err := c.getPurgeInfos()
	if err != nil {
		return
	}
	// 创建指标（优化后：不包含额外标签）
	for _, info := range purgeInfos {
		ch <- prometheus.MustNewConstMetric(
			c.purgeObjects,
			prometheus.GaugeValue,
			float64(info.ObjNum),
			Hostname,
		)
	}
}

// getPurgeInfos 获取回滚段信息
func (c *PurgeCollector) getPurgeInfos() ([]PurgeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.dbPool.QueryContext(ctx, QueryPurgeInfoSqlStr)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryPurgeInfoSqlStr, err)
		return nil, err
	}
	defer rows.Close()

	var purgeInfos []PurgeInfo
	for rows.Next() {
		var info PurgeInfo
		// 跳过不需要的字段，只扫描 ObjNum
		var isRunning, purgeForTs sql.NullString
		err := rows.Scan(&info.ObjNum, &isRunning, &purgeForTs)
		if err != nil {
			logger.Errorf("[QueryPurgeInfoSqlStr] Error scanning purge row: %v", err)
			continue
		}
		purgeInfos = append(purgeInfos, info)
	}

	if err = rows.Err(); err != nil {
		logger.Errorf("Error iterating purge rows: %v", err)
		return nil, err
	}

	return purgeInfos, nil
}
