package dm8

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type TableSpaceDateFileInfoCollector struct {
	db        *sql.DB
	totalDesc *prometheus.Desc
	freeDesc  *prometheus.Desc
	config    *Config
}

type TableSpaceDateFileInfo struct {
	Path       string
	TotalSize  float64
	FreeSize   float64
	AutoExtend string
	NextSize   string
	MaxSize    string
}

func NewTableSpaceDateFileInfoCollector(db *sql.DB, config *Config) MetricCollector {
	return &TableSpaceDateFileInfoCollector{
		db:     db,
		config: config,
		totalDesc: prometheus.NewDesc(
			dmdbms_tablespace_file_total_info,
			"Tablespace file information",
			[]string{"host_name", "tablespace_name", "auto_extend", "next_size", "max_size"}, // 添加标签
			nil,
		),
		freeDesc: prometheus.NewDesc(
			dmdbms_tablespace_file_free_info,
			"Tablespace file information",
			[]string{"host_name", "tablespace_name", "auto_extend", "next_size", "max_size"}, // 添加标签
			nil,
		),
	}
}

func (c *TableSpaceDateFileInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.totalDesc
	ch <- c.freeDesc
}

func (c *TableSpaceDateFileInfoCollector) Collect(ch chan<- prometheus.Metric) {

	//保存全局结果对象，可以用来做缓存以及序列化
	var tablespaceInfos []TableSpaceDateFileInfo

	// 从缓存中获取数据，使用带数据源的缓存键
	cacheKey := fmt.Sprintf("%s_%s", dmdbms_tablespace_file_total_info)
	if cachedJSON, found := GetFromCache(cacheKey); found {
		// 将缓存中的 JSON 字符串转换为 TablespaceInfo 切片
		if err := json.Unmarshal([]byte(cachedJSON), &tablespaceInfos); err != nil {
			// 处理反序列化错误
			logger.Errorf("[dmdbms_tablespace_file_total_info] failed to unmarshal cached data: %v", err)
			// 反序列化失败，忽略缓存中的数据，继续查询数据库
			cachedJSON = "" // 清空缓存数据，确保后续不使用过期或损坏的数据
		} else {
			//logger.Infof("Use cache TablespaceDateFile data")
			// 使用缓存的数据
			for _, info := range tablespaceInfos {
				ch <- prometheus.MustNewConstMetric(c.totalDesc, prometheus.GaugeValue, info.TotalSize, Hostname, info.Path, info.AutoExtend, info.NextSize, info.MaxSize)
				ch <- prometheus.MustNewConstMetric(c.freeDesc, prometheus.GaugeValue, info.FreeSize, Hostname, info.Path, info.AutoExtend, info.NextSize, info.MaxSize)
			}
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QueryTablespaceFileSqlStr)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryTablespaceFileSqlStr, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var info TableSpaceDateFileInfo
		if err := rows.Scan(&info.Path, &info.TotalSize, &info.FreeSize, &info.AutoExtend, &info.NextSize, &info.MaxSize); err != nil {
			logger.Errorf("Error scanning row has error: %s ", err)
			continue
		}
		tablespaceInfos = append(tablespaceInfos, info)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("Error with rows has error: %s ", err)
	}
	// 发送数据到 Prometheus
	for _, info := range tablespaceInfos {
		ch <- prometheus.MustNewConstMetric(c.totalDesc, prometheus.GaugeValue, info.TotalSize, Hostname, info.Path, info.AutoExtend, info.NextSize, info.MaxSize)
		ch <- prometheus.MustNewConstMetric(c.freeDesc, prometheus.GaugeValue, info.FreeSize, Hostname, info.Path, info.AutoExtend, info.NextSize, info.MaxSize)
	}

	// 将 TablespaceInfo 切片序列化为 JSON 字符串
	valueJSON, err := json.Marshal(tablespaceInfos)
	if err != nil {
		// 处理序列化错误
		logger.Errorf("TablespaceInfo marshal error: %s", err)
		return
	}
	// 将查询结果存入缓存，重用之前定义的cacheKey
	SetCache(cacheKey, string(valueJSON), time.Minute*(c.config.BigKeyDataCacheTime))
	//logger.Infof("TablespaceFileInfoCollector exec finish")

}
