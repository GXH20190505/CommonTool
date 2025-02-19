package oraclebase


import (
	"context"
	"database/sql"
	"fmt"
	"time"
	_"github.com/godror/godror"
	"strings"
	"stringbase"
)

type DBHandler struct {
	DB *sql.DB
}

// NewDBHandler 初始化一个数据库连接
func NewDBHandler(username, password, connectionString string) (*DBHandler, error) {
	dsn := fmt.Sprintf("%s/%s@%s", username, password, connectionString)
	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(context.Background()); err != nil {
		return nil, err
	}

	return &DBHandler{DB: db}, nil
}

// Close 关闭数据库连接
func (dh *DBHandler) Close() error {
	return dh.DB.Close()
}

// ExecuteNonQuery 执行非查询SQL语句（增删改）
func (dh *DBHandler) ExecuteNonQuery(ctx context.Context, query string, args ...interface{}) (int64, error) {
	stmt, err := dh.DB.PrepareContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affected, nil
}
func (self *DBHandler) InsertData(tabName string, HTCols []map[string]string) (string, sql.Result, error) {
	var strCol string
	var strVal string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for c, v := range HTCols[0] {
		// 对于 Oracle，字符串和日期等类型的数据需要用单引号包裹，同时需要对单引号进行转义
		if v == "" || strings.ToUpper(v) == "NULL" {
			strVal += fmt.Sprintf("NULL,")
		}else if strings.Contains(c,"TIME"){
			escapedValue := strings.ReplaceAll(v, "'", "''") // 对单引号进行转义
			strVal += fmt.Sprintf("TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),", escapedValue)
		} else {
			escapedValue := strings.ReplaceAll(v, "'", "''") // 对单引号进行转义
			strVal += fmt.Sprintf("'%s',", escapedValue)
		}
		strCol += fmt.Sprintf("%s,", c)
	}

	if strCol == "" || strVal == "" {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	// 删除末尾的逗号
	strsql := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)", tabName, strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}

func getColumnNames(data map[string]string) string {
	columns := make([]string, 0, len(data))
	for k := range data {
		columns = append(columns, k)
	}
	return strings.Join(columns, ",")
}

func getBindVariables(data map[string]string) string {
	variables := make([]string, 0, len(data))
	for range data {
		variables = append(variables, `?`)
	}
	return strings.Join(variables, ",")
}
func (self *DBHandler) ExecuteSql(strSql string) (sql.Result, error) {
	//fmt.Println(strSql)
	res, err := self.DB.Exec(strSql)
	return res, err
}
func (dh *DBHandler) ExecuteNonQueryWithMap(tableName string, data []map[string]string) (int64, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	colNames := make([]string, 0, len(data))
	colValues := make([]string, 0, len(data))

	for col, val := range data[0] {
		colNames = append(colNames, col)
		colValues = append(colValues, "'"+val+"'")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(colNames, ","), strings.Join(colValues, ","))

	res, err := dh.DB.ExecContext(ctx, query)
	if err != nil {
		return 0, query, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, query, err
	}

	return affected, query, nil
}
// ExecuteQuery 执行查询SQL语句
func (dh *DBHandler) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (DataTable, error) {
	var result DataTable

	stmt, err := dh.DB.PrepareContext(ctx, query)
	if err != nil {
		return result, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return result, err
	}
	result.Columns = columns

	// 填充行数据
	for rows.Next() {
		rowData := make(map[string]string)
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return result, err
		}
		for i, col := range columns {
			val := values[i]
			switch v := val.(type) {
			case nil:
				rowData[strings.ToUpper(col)] = "NULL"
			case []byte:
				rowData[strings.ToUpper(col)] = string(v)
			default:
				rowData[strings.ToUpper(col)] = fmt.Sprintf("%v", v)
			}
		}
		result.RowData = append(result.RowData, rowData)
		result.Count++
	}

	return result, nil
}
/** 获取表字段
返回值：所有表字段的字符串，格式：[field1][field2][...]
*/
func (self *DBHandler) GetColumns(tableName string) string {
	ctx, cancel := context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()
	strSql := fmt.Sprintf("SELECT column_name as FIELD FROM all_tab_columns WHERE table_name = '%s'", tableName)
	dt, err := self.ExecuteQuery(ctx,strSql)
	if err != nil {
		return ""
	}
	var cols strings.Builder
	for i := 0; i < dt.Count; i++ {
		cols.WriteString(fmt.Sprintf("[%s]", dt.RowData[i]["FIELD"]))
	}
	return cols.String()
}
// QueryRow 获取单行结果
func (dh *DBHandler) QueryRow(ctx context.Context, query string, args ...interface{}) (*sql.Row, error) {
	return dh.DB.QueryRowContext(ctx, query, args...), nil
}

// Insert 插入数据示例
func (dh *DBHandler) Insert(tableName string, data map[string]interface{}) (int64, error) {
	fields, values := make([]string, 0, len(data)), make([]interface{}, 0, len(data))

	for k, v := range data {
		fields = append(fields, k)
		values = append(values, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, joinStrings(fields, ","), joinPlaceholders(len(fields)))

	res, err := dh.ExecuteNonQuery(context.Background(), query, values...)
	if err != nil {
		return 0, err
	}

	return res, nil
}

func (dh *DBHandler) TabExist(name string)(bool,error) {
	ctx, cancel := context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()
	// 修改这里的表名
	query :=fmt.Sprintf(`SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = '%s'`,name)
	var count int64
	rows, err := dh.QueryRow(ctx,query)
	if err != nil {
		return false, fmt.Errorf("执行查询时出错: %w", err)
	}
	err = rows.Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
func (oc *DBHandler) DuplicateSliceData2(tabName string, data []map[string]string) (string, []sql.Result, error) {
	tx, err := oc.DB.Begin()
	if err != nil {
		return "", nil, err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	var results []sql.Result
	var keys []string
	for k := range data[0] {
		keys = append(keys, k)
	}

	for _, row := range data {
		// 构建每行的参数和SQL语句
		var placeholders []string
		var args []interface{}
		for _, key := range keys {
			if val, ok := row[key]; ok {
				placeholders = append(placeholders, "?")
				args = append(args, val)
			} else {
				// 如果某行数据缺少某个列值，需要处理这种情况（例如添加默认值或跳过这一行）
				return "", nil, fmt.Errorf("Missing value for key '%s' in row", key)
			}
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tabName, strings.Join(keys, ","), strings.Join(placeholders, ","))

		stmt, execErr := tx.Prepare(query)
		if execErr != nil {
			tx.Rollback()
			return "", nil, execErr
		}
		defer stmt.Close()

		// 执行单行插入
		result, execResultErr := stmt.Exec(args...)
		if execResultErr != nil {
			tx.Rollback()
			return "", nil, execResultErr
		}

		results = append(results, result)
	}

	return "", results, nil
}
func (oc *DBHandler) DuplicateSliceDataOracle(tabName string, data []map[string]string) ([]sql.Result, error) {
	tx, err := oc.DB.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	var results []sql.Result
	var keys []string
	for k := range data[0] {
		keys = append(keys, k)
	}

	for _, row := range data {
		var placeholders []string
		var args []interface{}
		for _, key := range keys {
			if val, ok := row[key]; ok {
				if strings.Contains(key, "TIME"){
						args=append(args,fmt.Sprintf("TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')",val ))
						placeholders = append(placeholders, ":"+key)

				}else if strings.Contains(key, "VALUE") {
					args=append(args,stringbase.Float64(val))
					placeholders = append(placeholders, ":"+key)
				}else {
					args = append(args, val)
					placeholders = append(placeholders, ":"+key)
				}

			} else {
				return nil, fmt.Errorf("Missing value for key '%s' in row", key)
			}
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tabName, strings.Join(keys, ","), strings.Join(placeholders, ","))
		stmt, execErr := tx.Prepare(query)
		if execErr != nil {
			tx.Rollback()
			return nil, execErr
		}
		defer stmt.Close()

		// 执行单条插入
		result, execResultErr := stmt.Exec(args...)
		if execResultErr != nil {
			tx.Rollback()
			return nil, execResultErr
		}

		results = append(results, result)
	}

	return results, nil
}
func (oc *DBHandler) DuplicateSliceData(tabName string, data []map[string]string) (string, sql.Result, error) {
	var keys []string
	var values []string
	var placeholders []string
	var args []interface{}

	for k := range data[0] {
		keys = append(keys, k)
	}

	for _, row := range data {
		var valueStrings []string
		for _, v := range keys {
			valueStrings = append(valueStrings, "'" + row[v] + "'")
		}
		values = append(values, "(" + strings.Join(valueStrings, ",") + ")")

		for _, v := range row {
			args = append(args, v)
		}
		placeholders = append(placeholders, "(" + strings.TrimRight(strings.Repeat("?,", len(keys)), ",") + ")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tabName, strings.Join(keys, ","), strings.Join(values, ","))

	count,err:= oc.ExecuteNonQuery(context.Background(), query)
	fmt.Println(count)
	if err != nil {
		return query, nil, err
	}
	return query, nil, nil
}
func (oc *DBHandler) DuplicateSliceData3(tabName string, data []map[string]string) (string, sql.Result, error) {
	tx, err := oc.DB.Begin()
	if err != nil {
		return "", nil, err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	var keys []string
	var values []string
	var placeholders []string
	var args []interface{}

	for k := range data[0] {
		keys = append(keys, k)
	}

	for _, row := range data {
		var valueStrings []string
		for _, v := range keys {
			valueStrings = append(valueStrings, "'" + row[v] + "'")
		}
		values = append(values, "(" + strings.Join(valueStrings, ",") + ")")

		for _, v := range row {
			args = append(args, v)
		}
		placeholders = append(placeholders, "(" + strings.TrimRight(strings.Repeat("?,", len(keys)), ",") + ")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tabName, strings.Join(keys, ","), strings.Join(values, ","))

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return "", nil, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(args...)
	if err != nil {
		tx.Rollback()
		return "", nil, err
	}

	err = tx.Commit()
	if err != nil {
		return "", nil, err
	}

	return query, result, nil
}
// 辅助函数，将map转换为params切片
func rowToParams(row map[string]string) []interface{} {
	params := make([]interface{}, 0, len(row))
	for _, val := range row {
		params = append(params, val)
	}
	return params
}
// joinStrings 合并字符串数组为逗号分隔的字符串
func joinStrings(arr []string, sep string) string {
	return strings.Join(arr, sep)
}

// joinPlaceholders 创建占位符字符串
func joinPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ",")
}

