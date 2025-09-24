package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// 定义数据结构
type InstanceLogInfo struct {
	Txt     sql.NullString
	Level   sql.NullString
	Pid     sql.NullString
	LogTime sql.NullString
}

// 定义收集器结构体
type DbInstanceLogErrorCollector struct {
	db                  *sql.DB
	instanceLogInfoDesc *prometheus.Desc
	config              *Config
}

// 初始化收集器
func NewInstanceLogErrorCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbInstanceLogErrorCollector{
		db:     db,
		config: config,
		instanceLogInfoDesc: prometheus.NewDesc(
			dmdbms_instance_log_error_info,
			"Information about DM database Instance error log info",
			[]string{"pid", "level", "log_time", "txt"},
			nil,
		),
	}
}

// Describe 方法
func (c *DbInstanceLogErrorCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.instanceLogInfoDesc
}

func (c *DbInstanceLogErrorCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QueryInstanceErrorLogSql)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryInstanceErrorLogSql, err)
		return
	}
	defer rows.Close()

	var instanceLogInfos []InstanceLogInfo
	for rows.Next() {
		var info InstanceLogInfo
		//LOG_TIME,PID,LEVEL$,TXT
		if err := rows.Scan(&info.LogTime, &info.Pid, &info.Level, &info.Txt); err != nil {
			logger.Errorf("[QueryInstanceErrorLogSql] Error scanning row,error: %s", err)
			continue
		}
		instanceLogInfos = append(instanceLogInfos, info)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf("[QueryInstanceErrorLogSql] Error with rows error: %s", err)
	}

	// 对instanceLogInfos进行去重处理
	instanceLogInfos = removeDuplicateLogInfos(instanceLogInfos)

	// 发送数据到 Prometheus
	for _, info := range instanceLogInfos {
		//[]string{"pid", "level", "log_time", "txt"}

		pid := NullStringToString(info.Pid)
		level := NullStringToString(info.Level)
		logTime := NullStringToString(info.LogTime)
		txt := NullStringToString(info.Txt)

		//ps: log日志本身就是异常的,所以统一设置为1
		logStatusValue := 1

		ch <- prometheus.MustNewConstMetric(
			c.instanceLogInfoDesc,
			prometheus.GaugeValue,
			float64(logStatusValue),
			pid, level, logTime, txt,
		)
	}
}

// 移除重复的日志记录（保留原始顺序）
func removeDuplicateLogInfos(logs []InstanceLogInfo) []InstanceLogInfo {
	// 使用map来跟踪已经看到的日志
	seen := make(map[string]bool)
	result := []InstanceLogInfo{} // 保留原始顺序的结果集

	// 按原始顺序遍历，只保留第一次出现的元素
	for _, info := range logs {
		// 为每条日志创建一个唯一标识
		pid := NullStringToString(info.Pid)
		level := NullStringToString(info.Level)
		logTime := NullStringToString(info.LogTime)
		txt := NullStringToString(info.Txt)

		key := pid + "|" + level + "|" + logTime + "|" + txt

		// 如果这个日志已经见过，则跳过
		if seen[key] {
			continue
		}

		// 标记为已见，并添加到结果中
		seen[key] = true
		result = append(result, info)
	}

	return result
}
