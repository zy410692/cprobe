package dm8

import (
	"context"
	"database/sql"
	"github.com/cprobe/cprobe/lib/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// 定义数据结构
type UserInfo struct {
	Username          sql.NullString
	ReadOnly          sql.NullString
	AccountStatus     sql.NullString
	ExpiryDate        sql.NullString
	ExpiryDateDay     sql.NullString
	DefaultTablespace sql.NullString
	Profile           sql.NullString
	CreateTime        sql.NullString
}

// 定义收集器结构体
type DbUserCollector struct {
	db               *sql.DB
	userListInfoDesc *prometheus.Desc
	config           *Config
}

func NewDbUserCollector(db *sql.DB, config *Config) MetricCollector {
	return &DbUserCollector{
		db:     db,
		config: config,
		userListInfoDesc: prometheus.NewDesc(
			dmdbms_user_list_info,
			"Information about DM database users",
			[]string{"host_name", "username", "read_only", "expiry_date", "expiry_date_day", "default_tablespace", "profile", "create_time"},
			nil,
		),
	}
}

func (c *DbUserCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.userListInfoDesc
}

func (c *DbUserCollector) Collect(ch chan<- prometheus.Metric) {

	ctx, cancel := context.WithTimeout(context.Background(), c.config.QueryTimeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, QueryUserInfoSqlStr)
	if err != nil {
		handleDbQueryErrorWithSQL(QueryUserInfoSqlStr, err)
		return
	}
	defer rows.Close()

	var userInfos []UserInfo
	for rows.Next() {
		var info UserInfo
		if err := rows.Scan(&info.Username, &info.ReadOnly, &info.AccountStatus, &info.ExpiryDate, &info.ExpiryDateDay, &info.DefaultTablespace, &info.Profile, &info.CreateTime); err != nil {
			logger.Errorf("Error scanning row has error: %s", err)
			continue
		}
		userInfos = append(userInfos, info)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf("Error with rows has error: %s ", err)
		return
	}

	// 发送数据到 Prometheus
	for _, info := range userInfos {
		username := NullStringToString(info.Username)
		readOnly := NullStringToString(info.ReadOnly)
		expiryDate := NullStringToString(info.ExpiryDate)
		expiryDateDay := NullStringToString(info.ExpiryDateDay)
		defaultTablespace := NullStringToString(info.DefaultTablespace)
		profile := NullStringToString(info.Profile)
		createTime := NullStringToString(info.CreateTime)

		// 判断 AccountStatus 的值
		accountStatusValue := 0.0
		if NullStringToString(info.AccountStatus) == "锁定" {
			accountStatusValue = 1.0
		}

		ch <- prometheus.MustNewConstMetric(
			c.userListInfoDesc,
			prometheus.GaugeValue,
			accountStatusValue,
			Hostname, username, readOnly, expiryDate, expiryDateDay, defaultTablespace, profile, createTime,
		)
	}
}
