package dm8

import (
	"context"
	"database/sql"

	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// DBSystemInfoCollector 数据库系统信息采集器结构体
type DBSystemInfoCollector struct {
	db                 *sql.DB
	systemBaseInfoDesc *prometheus.Desc // 系统基础信息（始终为1，包含所有信息在标签中）
	cpuInfoDesc        *prometheus.Desc // CPU核心数信息
	memoryInfoDesc     *prometheus.Desc // 内存大小信息
	config             *Config
}

// SystemInfo 系统信息结构体
type SystemInfo struct {
	NCpu          sql.NullFloat64
	TotalPhySize  sql.NullFloat64
	TotalVirSize  sql.NullFloat64
	TotalDiskSize sql.NullFloat64
}

// NewDBSystemInfoCollector 创建数据库系统信息采集器
func NewDBSystemInfoCollector(db *sql.DB, config *Config) MetricCollector {
	return &DBSystemInfoCollector{
		db:     db,
		config: config,
		systemBaseInfoDesc: prometheus.NewDesc(
			dmdbms_system_base_info,
			"Database system base information metrics (always 1 with system info in labels)",
			[]string{"host_name", "n_cpu", "total_phy_size", "total_vir_size", "total_disk_size"},
			nil,
		),
		cpuInfoDesc: prometheus.NewDesc(
			dmdbms_system_cpu_info,
			"Number of CPU cores",
			[]string{"host_name"},
			nil,
		),
		memoryInfoDesc: prometheus.NewDesc(
			dmdbms_system_memory_info,
			"Total physical memory size in bytes",
			[]string{"host_name"},
			nil,
		),
	}
}

// Describe 描述指标
func (c *DBSystemInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.systemBaseInfoDesc
	ch <- c.cpuInfoDesc
	ch <- c.memoryInfoDesc
}

// Collect 采集指标
func (c *DBSystemInfoCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	// 使用统一的SQL查询获取系统信息
	rows, err := c.db.QueryContext(ctx, QuerySystemInfoSqlStr)
	if err != nil {
		handleDbQueryErrorWithSQL(QuerySystemInfoSqlStr, err)
		return
	}
	defer rows.Close()

	var info SystemInfo

	if rows.Next() {
		if err := rows.Scan(&info.NCpu, &info.TotalPhySize, &info.TotalVirSize, &info.TotalDiskSize); err != nil {
			logger.Errorf("Error scanning system info row has error: %s", err)
			return
		}
	}

	if err := rows.Err(); err != nil {
		logger.Errorf("Error with system info rows has error: %s", err)
		return
	}

	// 1. 发送数据库系统基础信息指标（始终为1，系统信息在标签中）
	nCpuStr := ""
	totalPhySizeStr := ""
	totalVirSizeStr := ""
	totalDiskSizeStr := ""

	if info.NCpu.Valid {
		nCpuStr = NullFloat64ToString(info.NCpu)
	}
	if info.TotalPhySize.Valid {
		totalPhySizeStr = NullFloat64ToString(info.TotalPhySize)
	}
	if info.TotalVirSize.Valid {
		totalVirSizeStr = NullFloat64ToString(info.TotalVirSize)
	}
	if info.TotalDiskSize.Valid {
		totalDiskSizeStr = NullFloat64ToString(info.TotalDiskSize)
	}

	ch <- prometheus.MustNewConstMetric(
		c.systemBaseInfoDesc,
		prometheus.GaugeValue,
		1, // 始终为1，如用户要求
		nCpuStr,
		totalPhySizeStr,
		totalVirSizeStr,
		totalDiskSizeStr,
		Hostname,
	)

	// 2. 发送CPU核心数指标
	if info.NCpu.Valid {
		ch <- prometheus.MustNewConstMetric(
			c.cpuInfoDesc,
			prometheus.GaugeValue,
			NullFloat64ToFloat64(info.NCpu),
			Hostname,
		)
	}

	// 3. 发送内存大小指标
	if info.TotalPhySize.Valid {
		ch <- prometheus.MustNewConstMetric(
			c.memoryInfoDesc,
			prometheus.GaugeValue,
			NullFloat64ToFloat64(info.TotalPhySize),
			Hostname,
		)
	}
}
