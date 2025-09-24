package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// 定义数据结构
type RapplyTimeDiff struct {
	TimeDiff sql.NullFloat64
}

// 定义收集器结构体
type DbRapplyTimeDiffCollector struct {
	db           *sql.DB
	timeDiffDesc *prometheus.Desc
	config       *Config
}

func NewDbRapplyTimeDiffCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbRapplyTimeDiffCollector{
		db:     db,
		config: config,
		timeDiffDesc: prometheus.NewDesc(
			dmdbms_rapply_time_diff,
			"Time difference in seconds between APPLY_CMT_TIME and LAST_CMT_TIME from V$RAPPLY_STAT",
			[]string{"host_name"},
			nil,
		),
	}
}

func (c *DbRapplyTimeDiffCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.timeDiffDesc
}

func (c *DbRapplyTimeDiffCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	// 执行查询
	rows, err := c.db.QueryContext(ctx, QueryRapplyTimeDiffSql)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryRapplyTimeDiffSql, err)
		return
	}
	defer rows.Close()

	var rapplyTimeDiffs []RapplyTimeDiff
	for rows.Next() {
		var info RapplyTimeDiff
		if err := rows.Scan(&info.TimeDiff); err != nil {
			logger.Errorf("Error scanning row has error: %s", err)
			continue
		}
		rapplyTimeDiffs = append(rapplyTimeDiffs, info)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("Error with rows has error %s", err)
		return
	}

	for _, info := range rapplyTimeDiffs {
		ch <- prometheus.MustNewConstMetric(
			c.timeDiffDesc,
			prometheus.GaugeValue,
			NullFloat64ToFloat64(info.TimeDiff),
			Hostname,
		)
	}
}
