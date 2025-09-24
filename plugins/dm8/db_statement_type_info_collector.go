package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// 定义数据结构
type DbSqlExecTypeInfo struct {
	Name    sql.NullString
	StatVal sql.NullFloat64
}

// 定义收集器结构体
type DbSqlExecTypeCollector struct {
	db                *sql.DB
	statementTypeDesc *prometheus.Desc
	config            *Config
}

func NewDbSqlExecTypeCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbSqlExecTypeCollector{
		db:     db,
		config: config,
		statementTypeDesc: prometheus.NewDesc(
			dmdbms_statement_type_total,
			"Information about different types of statements",
			[]string{"host_name", "statement_name"},
			nil,
		),
	}

}

func (c *DbSqlExecTypeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.statementTypeDesc
}

func (c *DbSqlExecTypeCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QuerySqlExecuteCountSqlStr)
	if err != nil {
		handleDbQueryErrorWithSQL(QuerySqlExecuteCountSqlStr, err)
		return
	}
	defer rows.Close()

	var sysstatInfos []DbSqlExecTypeInfo
	for rows.Next() {
		var info DbSqlExecTypeInfo
		if err := rows.Scan(&info.Name, &info.StatVal); err != nil {
			logger.Errorf("Error scanning row has error: %s", err)
			continue
		}
		sysstatInfos = append(sysstatInfos, info)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf("Error with rows has error: %s ", err)
	}
	// 发送数据到 Prometheus
	for _, info := range sysstatInfos {
		statementName := NullStringToString(info.Name)

		ch <- prometheus.MustNewConstMetric(
			c.statementTypeDesc,
			prometheus.CounterValue,
			NullFloat64ToFloat64(info.StatVal),
			Hostname, statementName,
		)
	}
}
