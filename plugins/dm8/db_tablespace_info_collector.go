package dm8

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type TableSpaceInfoCollector struct {
	db        *sql.DB
	totalDesc *prometheus.Desc
	freeDesc  *prometheus.Desc
	config    *Config
}

type TableSpaceInfo struct {
	TablespaceName string
	TotalSize      float64
	FreeSize       float64
}

func NewTableSpaceInfoCollector(db *sql.DB, config *Config) MetricCollector {
	return &TableSpaceInfoCollector{
		db:     db,
		config: config,
		totalDesc: prometheus.NewDesc(
			dmdbms_tablespace_size_total_info,
			"Tablespace info information",
			[]string{"host_name", "tablespace_name"}, // 添加标签
			nil,
		),
		freeDesc: prometheus.NewDesc(
			dmdbms_tablespace_size_free_info,
			"Tablespace info information",
			[]string{"host_name", "tablespace_name"}, // 添加标签
			nil,
		),
	}
}

func (c *TableSpaceInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.totalDesc
	ch <- c.freeDesc
}

func (c *TableSpaceInfoCollector) Collect(ch chan<- prometheus.Metric) {

	//保存全局结果对象，可以用来做缓存以及序列化
	var tablespaceInfos []TableSpaceInfo

	// 从缓存中获取数据，使用带数据源的缓存键
	cacheKey := dmdbms_tablespace_size_total_info
	if cachedJSON, found := GetFromCache(cacheKey); found {
		// 将缓存中的 JSON 字符串转换为 TablespaceInfo 切片
		if err := json.Unmarshal([]byte(cachedJSON), &tablespaceInfos); err != nil {
			// 处理反序列化错误
			logger.Errorf("Error unmarshaling cached data has error: %s ", err)
			// 反序列化失败，忽略缓存中的数据，继续查询数据库
			cachedJSON = "" // 清空缓存数据，确保后续不使用过期或损坏的数据
		} else {
			//logger.Infof("Use cache TablespaceInfo data")
			// 使用缓存的数据
			for _, info := range tablespaceInfos {
				ch <- prometheus.MustNewConstMetric(c.totalDesc, prometheus.GaugeValue, info.TotalSize, Hostname, info.TablespaceName)
				ch <- prometheus.MustNewConstMetric(c.freeDesc, prometheus.GaugeValue, info.FreeSize, Hostname, info.TablespaceName)
			}
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QueryTablespaceInfoSqlStr)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryTablespaceInfoSqlStr, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var info TableSpaceInfo
		if err := rows.Scan(&info.TablespaceName, &info.TotalSize, &info.FreeSize); err != nil {
			logger.Errorf("Error scanning row has error: %s", err)
			continue
		}
		tablespaceInfos = append(tablespaceInfos, info)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("Error with rows has error: %s", err)
	}
	// 发送数据到 Prometheus
	for _, info := range tablespaceInfos {
		ch <- prometheus.MustNewConstMetric(c.totalDesc, prometheus.GaugeValue, info.TotalSize, Hostname, info.TablespaceName)
		ch <- prometheus.MustNewConstMetric(c.freeDesc, prometheus.GaugeValue, info.FreeSize, Hostname, info.TablespaceName)
	}

	// 将 TablespaceInfo 切片序列化为 JSON 字符串
	valueJSON, err := json.Marshal(tablespaceInfos)
	if err != nil {
		// 处理序列化错误
		logger.Errorf("TablespaceInfo marshal error: %s", err)
		return
	}
	// 将查询结果存入缓存，重用之前定义的cacheKey
	SetCache(cacheKey, string(valueJSON), time.Minute*c.config.BigKeyDataCacheTime)
	//	logger.Infof("TablespaceFileInfo exec finish")

}
