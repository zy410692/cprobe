package dm8

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DB_ARCH_NO_ENABLE = -1
	DB_ARCH_VALID     = 1
	DB_ARCH_INVALID   = 2
)

// DbArchStatusInfo 归档状态信息
type DbArchStatusInfo struct {
	archType   sql.NullString
	archDest   sql.NullString
	archSrc    sql.NullString
	archStatus sql.NullFloat64
}

type DbArchStatusCollector struct {
	db             *sql.DB
	archStatusDesc *prometheus.Desc
	archStatusInfo *prometheus.Desc // 归档所有状态
	config         *Config
}

func NewDbArchStatusCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbArchStatusCollector{
		db:     db,
		config: config,
		archStatusInfo: prometheus.NewDesc(
			dmdbms_arch_status_info,
			"Information about DM database archive status, value info: vaild = 1,invaild = 0",
			[]string{"arch_type", "arch_dest", "arch_src"},
			nil,
		),
		archStatusDesc: prometheus.NewDesc(
			dmdbms_arch_status,
			"Information about DM database archive status",
			[]string{"host_name"},
			nil,
		),
	}
}

func (c *DbArchStatusCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.archStatusDesc
	ch <- c.archStatusInfo
}

func (c *DbArchStatusCollector) Collect(ch chan<- prometheus.Metric) {
	//funcStart := time.Now()
	//// 时间间隔的计算发生在 defer 语句执行时，确保能够获取到正确的函数执行时间。
	//defer func() {
	//	duration := time.Since(funcStart)
	//	logger.Infof("func exec time：%vms", duration.Milliseconds())
	//}()

	if err := c.db.Ping(); err != nil {
		logger.Errorf("Database connection is not available: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	// 获取数据库归档状态信息
	dbArchStatus, err := getDbArchStatus(ctx, c.db)
	if err != nil {
		logger.Errorf("exec getDbArchStatus func error", err)
		setArchMetric(ch, c.archStatusDesc, DB_ARCH_INVALID)
		return
	}
	setArchMetric(ch, c.archStatusDesc, dbArchStatus)
	// 如果归档开启，查询所有归档的状态信息
	if dbArchStatus == DB_ARCH_VALID {
		dbArchStatusInfos, err := c.getDbArchStatusInfo(ctx, c.db)
		if err != nil {
			logger.Errorf(fmt.Sprintf("exec getDbArchStatusInfo func error"), c.archStatusDesc.String())
			return
		}

		for _, dbArchStatusInfo := range dbArchStatusInfos {
			archType := NullStringToString(dbArchStatusInfo.archType)
			archDest := NullStringToString(dbArchStatusInfo.archDest)
			archSrc := NullStringToString(dbArchStatusInfo.archSrc)
			archStatus := NullFloat64ToFloat64(dbArchStatusInfo.archStatus)

			ch <- prometheus.MustNewConstMetric(
				c.archStatusInfo,
				prometheus.GaugeValue,
				archStatus,
				archType, archDest, archSrc,
			)
		}
	}
}

// 辅助函数：设置指标
func setArchMetric(ch chan<- prometheus.Metric, desc *prometheus.Desc, value int) {
	hostname := Hostname
	ch <- prometheus.MustNewConstMetric(
		desc,
		prometheus.GaugeValue,
		float64(value),
		hostname,
	)
}

// 获取数据库归档状态信息
func getDbArchStatus(ctx context.Context, db *sql.DB) (int, error) {
	var dbArchStatus string

	// 查询 PARA_VALUE
	query := `select /*+DMDB_CHECK_FLAG*/ PARA_VALUE from v$dm_ini where para_name='ARCH_INI'`
	row := db.QueryRowContext(ctx, query)
	err := row.Scan(&dbArchStatus)
	if err != nil {
		return DB_ARCH_INVALID, fmt.Errorf("query error: %v", err)
	}

	// 处理 PARA_VALUE 为 '1' 的情况
	if dbArchStatus == "1" {
		query = `select /*+DMDB_CHECK_FLAG*/ case arch_status when 'VALID' then 1 when 'INVALID' then 0 end ARCH_STATUS from v$arch_status where arch_type='LOCAL'`
		row = db.QueryRowContext(ctx, query)
		err = row.Scan(&dbArchStatus)
		if err != nil {
			return DB_ARCH_INVALID, fmt.Errorf("query error: %v", err)
		}
		if dbArchStatus == "1" {
			return DB_ARCH_VALID, nil
		} else if dbArchStatus == "0" {
			return DB_ARCH_INVALID, nil
		}
	} else if dbArchStatus == "0" {
		return DB_ARCH_NO_ENABLE, nil
	}

	logger.Infof("Check Database Arch Status Info Success")
	return DB_ARCH_INVALID, nil
}

// getDbArchStatusInfo 查询归档的所有状态信息
func (c *DbArchStatusCollector) getDbArchStatusInfo(ctx context.Context, db *sql.DB) ([]DbArchStatusInfo, error) {
	var dbArchStatusInfos []DbArchStatusInfo
	rows, err := db.QueryContext(ctx, QueryArchiveSendStatusSql)
	if err != nil {
		logger.Errorf("exec QueryArchiveSendStatus func error: %s,SQL: %s", err, QueryArchiveSendStatusSql)
		return dbArchStatusInfos, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbArchStatusInfo DbArchStatusInfo
		if err := rows.Scan(&dbArchStatusInfo.archStatus, &dbArchStatusInfo.archType,
			&dbArchStatusInfo.archDest, &dbArchStatusInfo.archSrc); err != nil {
			logger.Errorf("dm8: [getDbArchStatusInfo] Error scanning row")
			continue
		}
		dbArchStatusInfos = append(dbArchStatusInfos, dbArchStatusInfo)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf(fmt.Sprintf("dm8: [getDbArchStatusInfo] Error with rows"))
	}

	return dbArchStatusInfos, nil
}
