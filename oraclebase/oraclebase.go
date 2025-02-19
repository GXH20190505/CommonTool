// Oracle基础层
// 主要用于与 Oracle 数据库连接
package oraclebase

import (
	"database/sql"
	"fmt"
	//go_ora "github.com/sijms/go-ora/v2"
	//_ "github.com/godror/godror"
	"strconv"
	"stringbase"
	"strings"
	"timebase"
	"errors"
	"time"
	"errorlog"
)

/*
	数据库查询结果转换

把数据库中查询的Rows转换成DataTable格式，习惯操作
*/
type DataTable struct {
	Columns []string
	Count   int
	RowData []map[string]string //index=条数 key=Column value=value
}
type IndexData struct {
	RowData map[string]string //index=条数 key=Column value=value
}

// OracleCache 结构体
type OracleCache struct {
	connDB        *sql.DB
	ConnStr       string
	DBServiceName string
	DBInstance    string
	DBAddr        string
	DBUser        string
	DBPwd         string
}

// NewOracleConnection 创建并初始化Oracle数据库连接缓存，使用TNS连接
func NewOracleConnection(dbUser string, dbPwd string, dbServiceName string) (*OracleCache, error) {
	//dsn := "system/123456@orcl"
	//dsn := dbUser + "/" + dbPwd + "@" + dbServiceName
	var err error
	rs := &OracleCache{
		connDB:  nil,
		ConnStr: fmt.Sprintf("%s/%s@%s", dbUser, dbPwd, dbServiceName),
		DBUser:  dbUser,
		DBPwd:   dbPwd,
		//DBInstance: DBInstance,
	}
	// 创建数据库连接
	rs.connDB, err = sql.Open("godror", rs.ConnStr)
	// 检查连接是否成功
	err = rs.connDB.Ping()
	//if err != nil {
	//} else {
	//	fmt.Println("Connected to Oracle database via TNS.")
	//}
	rs.connDB.SetMaxIdleConns(20) //用于设置最大打开的连接数，默认值为0表示不限制
	rs.connDB.SetMaxOpenConns(20) //用于设置闲置的连接数
	//
	//rs.connDB.SetConnMaxLifetime(time.Second*30)
	return rs, err
}
// NewOracleConnection 创建并初始化Oracle数据库连接缓存，使用TNS连接
func NewOracleConnectionIP(dbUser string, dbPwd string, ip,port string) (*OracleCache, error) {
	//dsn := "system/123456@orcl"
	//dsn := dbUser + "/" + dbPwd + "@" + dbServiceName
	dsn := `user="irdm4k" password="admin123!@#qwe" connectString="192.168.100.22:1521/orcl"`

	// 设置连接池配置（可选）
	var err error
	rs := &OracleCache{
		connDB:  nil,
		ConnStr: dsn,
		DBUser:  dbUser,
		DBPwd:   dbPwd,
		//DBInstance: DBInstance,
	}
	// 创建数据库连接
	rs.connDB, err = sql.Open("godror", rs.ConnStr)
	// 检查连接是否成功
	err = rs.connDB.Ping()
	//if err != nil {
	//} else {
	//	fmt.Println("Connected to Oracle database via TNS.")
	//}
	rs.connDB.SetMaxIdleConns(50) //用于设置最大打开的连接数，默认值为0表示不限制
	rs.connDB.SetMaxOpenConns(50) //用于设置闲置的连接数
	//
	rs.connDB.SetConnMaxLifetime(time.Second*30)
	return rs, err
}

/*
CreateOracleSQLConnV2 用于创建数据库链接
*/
func CreateOracleSQLConnV2(dbconfigdatastr string) (*sql.DB, error) {
	// 建立 Oracle
	sqldbdata, err := sql.Open("oracle", dbconfigdatastr)
	if err != nil {
		fmt.Printf("sql application pool：%s\n", err)
		return nil, err
	}
	// 测试创建链接是否成功
	err = sqldbdata.Ping()
	dbName := strings.Split(dbconfigdatastr, "/")[2]
	if err != nil {
		fmt.Printf("%s DB Ping err : %s\n", dbName, err)
		defer sqldbdata.Close()
	} else {
		fmt.Printf("The Sql link is successful - %s. \n", dbName)
	}
	return sqldbdata, err
}

/*数据库插入等操作
 */
func (self *OracleCache) ExecuteSql(strSql string) (sql.Result, error) {
	//fmt.Println(strSql)
	res, err := self.connDB.Exec(strSql)
	return res, err
}

/*开启事务执行sql语句
 */
func (self *OracleCache) ExecuteTransactionSqls(strSqls ...string) (sql.Result, error) {
	tx, err := self.connDB.Begin()
	if err != nil {
		return nil, err
	}
	for _, sqlExec := range strSqls {
		res, err := tx.Exec(sqlExec)
		if err != nil {
			err = tx.Rollback()
			return res, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/* 数据库查询
 */
func (self *OracleCache) SelectSql(strsql string) (DataTable, error) {
	var dt DataTable
	dt.RowData = make([]map[string]string, 0)
	rows, err := self.connDB.Query(strsql)
	if err != nil {
		return dt, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return dt, err
	}
	dt.Columns = make([]string, len(columns))
	for i, col := range columns {
		dt.Columns[i] = strings.ToUpper(col)
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return dt, err
		}

		var value string
		var tempTab = make(map[string]string)
		for i, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			tempTab[dt.Columns[i]] = value
		}

		dt.RowData = append(dt.RowData, tempTab)
	}

	dt.Count = len(dt.RowData)
	return dt, nil
}

/** 判断表是否存在
返回值： false=表不存在，true=表存在
*/

func (self *OracleCache) TabExist(tableName string) bool {
	strSql := fmt.Sprintf(`SELECT COUNT(*) AS "COUNT" FROM USER_TABLES WHERE TABLE_NAME = '%s'`, tableName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return false
	}

	count, _ := strconv.Atoi(dt.RowData[0]["COUNT"])
	if count > 0 {
		return true
	}
	return false
}

// 获取表字段，返回值：所有表字段的字符串，格式：[field1][field2][...]
func (self *OracleCache) GetColumns(tableName string) string {
	strSql := fmt.Sprintf("SELECT column_name FROM user_tab_columns WHERE table_name = '%s'", tableName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return ""
	}
	var cols string
	for i := 0; i < dt.Count; i++ {
		cols += fmt.Sprintf("[%s]", dt.RowData[i]["COLUMN_NAME"])
	}
	return cols
}
// 获取表字段，返回值：所有表字段的字符串，格式：[field1][field2][...]
func (self *OracleCache) GetColumnArray(tableName string) []string {
	strSql := fmt.Sprintf("SELECT column_name FROM user_tab_columns WHERE table_name = '%s'", tableName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return nil
	}
	var cols []string
	for i := 0; i < dt.Count; i++ {
		cols=append(cols, dt.RowData[i]["COLUMN_NAME"])
	}
	return cols
}

// 获取表字段列表，返回值：字段名切片
func (self *OracleCache) GetColumnList(tabName string) []string {
	var cols []string
	strSql := fmt.Sprintf("SELECT column_name FROM user_tab_columns WHERE table_name = '%s'", tabName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return cols
	}
	for i := 0; i < dt.Count; i++ {
		cols = append(cols, dt.RowData[i]["COLUMN_NAME"])
	}
	return cols
}

// 获取所有表字段，返回值：key=tableName value=[field1][field2][...]，表名和列名均为大写
func (self *OracleCache) GetColumnsAll() map[string]string {
	strSql := fmt.Sprintf("SELECT column_name, table_name FROM all_tab_columns WHERE owner = UPPER('%s')", self.DBInstance)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return nil
	}
	TabColumns := make(map[string]string)
	for i := 0; i < dt.Count; i++ {
		TabColumns[strings.ToUpper(dt.RowData[i]["TABLE_NAME"])] += fmt.Sprintf("[%s]", dt.RowData[i]["COLUMN_NAME"])
	}

	return TabColumns
}

func (self *OracleCache) CreateColumns(tabName string, newCols map[string]string) (sql.Result, error) {
	if newCols == nil {
		return nil, fmt.Errorf("添加空字段")
	}
	/*var strSql strings.Builder
	strSql.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" ", tabName))
	for col, colType := range newCols {
		strSql.WriteString(fmt.Sprintf("ADD \"%s\" %s,", col, colType))
	}*/
	strSql := fmt.Sprintf("alter table %s ADD(", tabName)
	for col, colType := range newCols {
		col = strings.Replace(col, "[", "", -1)
		col = strings.Replace(col, "]", "", -1)
		strSql += fmt.Sprintf("%s %s,", col, colType)
	}

	// 移除最后一个逗号
	strSql = strSql[:len(strSql)-1]
	strSql+=")"
	fmt.Println(strSql)
	return self.ExecuteSql(strSql)
}
func (self *OracleCache) InsertData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	var strCol string
	var strVal string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for c, v := range HTCols {
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
func (self *OracleCache) InsertDataStr(strsql string) (string, sql.Result, error) {
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}
func (self *OracleCache) InsertDatabase(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	var strCol string
	var strVal string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for c, v := range HTCols {
		// 对于 Oracle，字符串和日期等类型的数据需要用单引号包裹，同时需要对单引号进行转义
		if v == "" || strings.ToUpper(v) == "NULL" {
			strVal += fmt.Sprintf("NULL,")
		} else {
			//if c == "DATETIME" {
			//
			//	escapedValue := strings.ReplaceAll(v, "'", "''") // 对单引号进行转义
			//	strVal += fmt.Sprintf("'%s',", escapedValue)
			//} else {
			//	escapedValue := strings.ReplaceAll(v, "'", "''") // 对单引号进行转义
			//	strVal += fmt.Sprintf("'%s',", escapedValue)
			//}
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

// 假设 ExecuteOracleSql 是一个执行 Oracle SQL 语句的方法，并且返回结果和错误
func (self *OracleCache) ReplaceData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	var strCols string
	var strVals string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for c, v := range HTCols {
		// 对于Oracle，确保列名用双引号包裹
		quotedCol := fmt.Sprintf(`"%s"`, c)
		strCols += fmt.Sprintf("%s,", quotedCol)

		if strings.ToUpper(v) == "NULL" {
			strVals += "NULL,"
		} else {
			// Oracle中字符串应该使用两个单引号来转义
			quotedVal := fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
			strVals += fmt.Sprintf("%s,", quotedVal)
		}
	}

	// 移除末尾多余的逗号
	strCols = strCols[:len(strCols)-1]
	strVals = strVals[:len(strVals)-1]

	// 构造 MERGE INTO 语句，这里假设有一个名为 pk_id 的主键用于判断是否更新或插入
	// 请根据实际情况替换 pk_id 为主键列名
	strsql := fmt.Sprintf(`MERGE INTO %s USING DUAL ON (%s = 1) 
                          WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`, tabName, "pk_id", strCols, strVals)

	// 执行 Oracle SQL 语句
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}

// 更新数据
// 返回值说明：SQL 语句，执行结果，错误信息
// 说明：使用的表需要有唯一索引
// 如果出现重复数据，则更新，否则插入
func (self *OracleCache) DuplicateData1(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	var strCol string
	var strVal string
	var strUp string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for c, v := range HTCols {
		strCol += fmt.Sprintf("%s,", c)
		if v == "" {
			strVal += fmt.Sprint("NULL,")
			strUp += fmt.Sprintf("%s=NULL,", c)
		} else {
			strVal += fmt.Sprintf("'%s',", v)
			strUp += fmt.Sprintf("%s='%s',", c, v)
		}
	}

	if strCol == "" || strVal == "" {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	//strsql := fmt.Sprintf("MERGE INTO %s t USING (SELECT %s FROM dual) s ON (t.primary_key = s.primary_key) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
	//	tabName, strCol[:len(strCol)-1], strUp[:len(strUp)-1], strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	//strsql := fmt.Sprintf("MERGE INTO %s t USING (SELECT %s FROM dual) s ON (t.primary_key = s.primary_key) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
	//	tabName, strCol[:len(strCol)-1], strUp[:len(strUp)-1], strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	strsql := fmt.Sprintf("MERGE INTO %s t USING (SELECT %s FROM dual) s ON (t.unique_key= s.unique_key) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
		tabName, strCol[:len(strCol)-1], strUp[:len(strUp)-1], strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	r, err := self.ExecuteSql(strsql)
	// errorlog.ErrorLogDebug("test_sql", "sql", strsql)
	return strsql, r, err
}

func (self *OracleCache) DuplicateData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	var columns []string
	var values []string
	var updates []string

	for c, v := range HTCols {

		columns = append(columns, fmt.Sprintf("\"t.\"")+c)
		if strings.ToUpper(v) == "NULL" || v == "" {
			values = append(values, "NULL")
			updates = append(updates, fmt.Sprintf("%s=NULL", c))
		} else {
			values = append(values, fmt.Sprintf("'%s'", v))
			updates = append(updates, fmt.Sprintf("t.%s='%s'", c, v))
		}
	}

	if len(columns) == 0 || len(values) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	columnsStr := strings.Join(columns, ",")
	valuesStr := strings.Join(values, ",")
	updatesStr := strings.Join(updates, ",")
	index, _ := self.GetTableUniqueKey(tabName, "")
	strsql := fmt.Sprintf("MERGE INTO %s t USING (SELECT %s FROM dual) s ON (t.%s = s.%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
		tabName, columnsStr, index, index, updatesStr, columnsStr, valuesStr)
	//columns[1]
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}

/*
* 批量新增和更新数据
返回值说明：sql语句，执行结果，错误信息
说明：使用的表需要有唯一索引
如果出现重复数据，则更新，否则插入
*/

func (self *OracleCache) DuplicateSliceData2(tabName string, data []map[string]string) (string, sql.Result, error) {
	mpCol := make(map[string]string)
	vals := make([]string, 0)
	var strUp string
	cols := make([]string, 0)
	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		for c := range HTCols {
			mpCol[c] = ""
		}
	}
	for col := range mpCol {
		cols = append(cols, fmt.Sprintf("\"%s\"", col))
		strUp += fmt.Sprintf("\"%s\"=s.\"%s\",", col, col)
	}
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		strVal := "("
		for _, col := range cols {
			key := strings.ReplaceAll(col, "\"", "")
			if HTCols[key] == "" {
				strVal += fmt.Sprint("NULL,")
			} else {
				strVal += fmt.Sprintf("'%s',", HTCols[key])
			}
		}
		vals = append(vals, strVal[:len(strVal)-1]+")")
	}

	if len(vals) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	// 构造 SQL 插入语句
	strsql := fmt.Sprintf("MERGE INTO %s t USING (SELECT %s FROM dual) s ON (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s",
		tabName, strings.Join(cols, ","), generateMergeCondition(cols), strUp[:len(strUp)-1], strings.Join(cols, ","), strings.Join(vals, ","))

	stmt, err := self.connDB.Prepare(strsql)
	if err != nil {
		return "", nil, err
	}
	defer stmt.Close()

	// 执行批量插入
	result, err := stmt.Exec()
	if err != nil {
		return "", nil, err
	}

	return strsql, result, nil
}


func (oc *OracleCache) DuplicateSliceDataOracle444(tabName string, data []map[string]string) ([]sql.Result, error) {
	tx, err := oc.connDB.Begin()
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

	// 准备插入语句
	keys := make([]string, 0, len(data[0]))
	for k := range data[0] {
		keys = append(keys, k)
	}

	fmt.Printf("%v\n", keys)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tabName, strings.Join(keys, ","), ":"+strings.Join(keys, ",:"),
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer stmt.Close()

	// 执行插入
	var results []sql.Result
	for _, row := range data {
		args := make([]interface{}, len(keys))
		for i, key := range keys {
			val, ok := row[key]
			if !ok {
				tx.Rollback()
				return nil, fmt.Errorf("missing value for key '%s' in row", key)
			}
			// 如果是时间字段，使用 TO_DATE 函数转换为 Oracle 的日期时间类型
			//if strings.Contains(key, "M_TIME") {
			if key == "M_TIME" {
				fmt.Printf("value key1:%v,%v\n", key, val)

				args[i] = fmt.Sprintf("TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')", val)
				fmt.Printf("DT:%v\n", args[i])
			} else if key == "M_VALUE" {
				//strings.Contains(key, "M_VALUE")
				fmt.Printf("value key2:%v,%v\n", key, val)
				// 如果是数字类型字段，尝试将其转换为 float64 类型
				//floatValue, err := strconv.ParseFloat(val, 64)
				//floatValue := stringbase.Float64(val)
				floatValue, err := strconv.ParseFloat(val, 64)

				if err != nil {
					tx.Rollback()
					return nil, fmt.Errorf("error converting value '%s' to float64: %v", val, err)
				}
				args[i] = floatValue
			} else {
				fmt.Printf("value key3:%v,%v\n", key, val)
				args[i] = val
			}
		}

		result, err := stmt.Exec(args...)
		if err != nil {
			tx.Rollback()
			fmt.Printf("675 err:%v,args:%v\n", err, args)
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}
func (oc *OracleCache) DuplicateSliceDataOracle0510(tabName string, data []map[string]string) ([]sql.Result, error) {
	tx, err := oc.connDB.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil || err != nil {
			tx.Rollback()
			errorlog.ErrorLogError("sqlError", "Commit", fmt.Sprintf("err:%v,r:%v ", err,r))
		} else {
			err = tx.Commit()
			if err!=nil{
				errorlog.ErrorLogError("sqlError", "Commit", fmt.Sprintf("%v ", err))
			}
		}
	}()

	var results []sql.Result
	var stmt *sql.Stmt
	for _, row := range data {
		// 动态构建插入语句
		keys := make([]string, 0)
		//values := make([]string, 0)
		args := make([]interface{}, 0)
		for key, val := range row {
			keys = append(keys, key)
			// 如果是时间字段，使用 TO_DATE 函数转换为 Oracle 的日期时间类型
			if strings.Contains(key, "TIME") {
				args =append(args, timebase.ParseInLocation(val))
				//fmt.Printf("DT:%v\n", args[i])
			} else if strings.Contains(key, "VALUE") {
				// 如果是数字类型字段，尝试将其转换为 float64 类型
				//floatValue, err := strconv.ParseFloat(val, 64)
				floatValue := stringbase.Float64(val)

				if err != nil {
					tx.Rollback()
					return nil, fmt.Errorf("error converting value '%s' to float64: %v", val, err)
				}
				args =append(args,floatValue)
			} else {
				//fmt.Printf("value key3:%v,%v\n", key, val)
				//args[i] = val
				args =append(args, fmt.Sprintf("%s", val))
			}
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",tabName, strings.Join(keys, ","), ":"+strings.Join(keys, ",:"),)
		//finalQuery := fmt.Sprintf("%s (%s)", query, strings.Join(strings.Fields(fmt.Sprint(args)), ", "))

		/*if stmt != nil {
			stmt.Close()
		}*/
		stmt, err = tx.Prepare(query)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		// 执行单行插入
		result, err := stmt.Exec(args...)
		if err != nil {
			tx.Rollback()
			detailedErrMsg := fmt.Sprintf("Insert failed with query: %s, arguments: %v. Error: %v", query, args, err)
			return nil, errors.New(detailedErrMsg)
		}
		affectedRows, err := result.RowsAffected()
		if err != nil {
			// 处理获取受影响行数时的错误
			fmt.Println("插入操作错误，新生成的ID为:", err)
		}
		if affectedRows <= 0 {
			errorlog.ErrorLogError("sqlError", "affectedRows", fmt.Sprintf("操作失败，影响了 %d 行，data:%v", affectedRows,row))
		}
		results = append(results, result)
	}
	if stmt != nil {
		stmt.Close()
	}
	return results, nil

}
func (oc *OracleCache) DuplicateSliceDataOracleReal(tabName string, data []map[string]string) ([]sql.Result, error) {
	tx, err := oc.connDB.Begin()
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
	// 准备插入语句

	keys := make([]string, 0)
	if strings.Contains(tabName, "_MINUTE")==false{
		for k := range data[0] {
			keys = append(keys, k)
		}
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tabName, strings.Join(keys, ","), ":"+strings.Join(keys, ",:"),
	)
	fmt.Printf("query:%v\n", query)
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer stmt.Close()
	// 执行插入
	var results []sql.Result
	for _, row := range data {
		args := make([]interface{}, len(keys))
		for i, key := range keys {
			val, ok := row[key]
			if !ok {
				tx.Rollback()
				return nil, fmt.Errorf("missing value for key '%s' in row", key)
			}
			// 如果是时间字段，使用 TO_DATE 函数转换为 Oracle 的日期时间类型
			if strings.Contains(key, "TIME") {
				args[i] =timebase.ParseInLocation(val)
			} else if strings.Contains(key, "VALUE") {
				// 如果是数字类型字段，尝试将其转换为 float64 类型
				floatValue := stringbase.Float64(val)
				if err != nil {
					tx.Rollback()
					return nil, fmt.Errorf("error converting value '%s' to float64: %v", val, err)
				}
				args[i] = floatValue
			} else {
				args[i] = fmt.Sprintf("%s", val)
			}
		}
		// 构建最终执行的插入语句字符串，用于错误时反馈
		finalQuery := fmt.Sprintf("%s (%s)", query, strings.Join(strings.Fields(fmt.Sprint(args)), ", "))
		result, err := stmt.Exec(args...)
		if err != nil {
			// 记录并返回详细的错误信息，包括执行失败的完整插入语句
			detailedErrMsg := fmt.Sprintf("Insert failed with query: %s, arguments: %v. Error: %v", finalQuery, args, err)
			tx.Rollback()
			return nil, errors.New(detailedErrMsg)
		}
		results = append(results, result)
	}
	return results, nil

}
func (oc *OracleCache) DuplicateSliceDataOracle(tabName string, data []map[string]string,tableColumns []string) ([]sql.Result, error) {
	tim:=time.Now()
	tx, err := oc.connDB.Begin()
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
		errorlog.ErrorLogDebug("insertDate",tabName,fmt.Sprintf("本次用时: 时间%d,条数%d",time.Now().Sub(tim).Milliseconds(), len(data)))
	}()
	var results []sql.Result
	var stmt *sql.Stmt
	for _, row := range data {
		// 动态构建插入语句
		args := make([]interface{}, 0)
		for _, col := range tableColumns {
			val, ok := row[col]
			if !ok {
				args=append(args,nil)// 填充 null
			} else {
				if strings.Contains(col, "TIME") {
					args =append(args, timebase.ParseInLocation(val))
					//fmt.Printf("DT:%v\n", args[i])
				} else if strings.Contains(col, "VALUE") {
					// 如果是数字类型字段，尝试将其转换为 float64 类型
					//floatValue, err := strconv.ParseFloat(val, 64)
					floatValue := stringbase.Float64(val)

					if err != nil {
						tx.Rollback()
						return nil, fmt.Errorf("error converting value '%s' to float64: %v", val, err)
					}
					args =append(args,floatValue)
				} else {
					//fmt.Printf("value key3:%v,%v\n", key, val)
					//args[i] = val
					args =append(args, fmt.Sprintf("%s", val))
				}
			}
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",tabName, strings.Join(tableColumns, ","), ":"+strings.Join(tableColumns, ",:"),)
		//query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tabName, strings.Join(tableColumns, ","), strings.Repeat("?,", len(tableColumns)))

		if stmt == nil {
			stmt, err = tx.Prepare(query)
			if err != nil {
				tx.Rollback()
				return nil, err
			}
		}

		// 执行单行插入
		result, err := stmt.Exec(args...)
		if err != nil {
			tx.Rollback()
			detailedErrMsg := fmt.Sprintf("Insert failed with query: %s, arguments: %v. Error: %v", query, args, err)
			return nil, errors.New(detailedErrMsg)
		}
		affectedRows, err := result.RowsAffected()
		if err != nil {
			// 处理获取受影响行数时的错误
			errorlog.ErrorLogDebug("sqlError","result.RowsAffected",fmt.Sprintf("插入操作错误，新生成的ID为:%v", err))
		}
		if affectedRows <= 0 {
			errorlog.ErrorLogError("sqlError", "affectedRows", fmt.Sprintf("操作失败，影响了 %d 行，data:%v", affectedRows, row))
		}
		results = append(results, result)
	}
	if stmt != nil {
		stmt.Close()
	}
	return results, nil
}


func (oc *OracleCache) DuplicateSliceDataOracleG(tabName string, data []map[string]string) ([]sql.Result, error) {
	tx, err := oc.connDB.Begin()
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

// 生成 MERGE INTO 语句的条件部分
func generateMergeCondition(keys []string) string {
	conditions := make([]string, len(keys))
	for i, key := range keys {
		conditions[i] = fmt.Sprintf("t.\"%s\" = s.\"%s\"", key, key)
	}
	return strings.Join(conditions, " AND ")
}

// 生成 MERGE INTO 语句的更新部分
func generateMergeUpdateSet(keys []string) string {
	updateSets := make([]string, len(keys))
	for _, key := range keys {
		updateSets = append(updateSets, fmt.Sprintf("\"%s\"=s.\"%s\"", key, key))
	}
	return strings.Join(updateSets, ",")
}

// 生成 MERGE INTO 语句的条件部分
//func generateMergeCondition(cols []string) string {
//	conditions := make([]string, len(cols))
//	for i, col := range cols {
//		conditions[i] = fmt.Sprintf("t.\"%s\" = s.\"%s\"", col, col)
//	}
//	return strings.Join(conditions, " AND ")
//}

/*
*
查询数据条数
返回查询条数
tabName = 表名
strWhere = 查询条件
返回值
-1 = 表示查询失败
0~N = 表示查询结果
*/
func (self *OracleCache) GetCount(tabName string, strWhere string) int {
	strsql := fmt.Sprintf("SELECT COUNT(*) AS COUNT FROM %s WHERE %s", tabName, strWhere)
	dt, err := self.SelectSql(strsql)
	if err != nil {
		return -1
	}
	for _, rows := range dt.RowData {
		s, _ := GetInteger(rows["COUNT"])
		return s
	}
	return 0
}

/*判断是否为合法数字且不为0*/
func GetInteger(value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("转换数据为空")
	}
	return strconv.Atoi(value)
}

// 根据列名删除所在行
func (self *OracleCache) DeleteData(tabName string, columnName string, columnValue string) (string, sql.Result, error) {
	strsql := fmt.Sprintf("DELETE FROM %s WHERE %s = '%s'", tabName, columnName, columnValue)
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}

// 获取该表的唯一索引, 'P' 表示主键约束，如果唯一键是通过主键定义的,'U'是唯一键约束
func (self *OracleCache) GetTableUniqueKey(tabName string, idexType string) (string, error) {

	//-- 'P' 表示主键约束，如果唯一键是通过主键定义的,'U'是唯一键约束
	if idexType == "" {
		idexType = "U"
	}
	strsql := fmt.Sprintf("SELECT cols.column_name FROM all_constraints cons JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name WHERE cons.constraint_type = '%s'   AND cons.table_name = '%s'", strings.ToUpper(idexType), tabName)
	r, err := self.SelectSql(strsql)
	var res string
	for _, v := range r.RowData {
		res = v["COLUMN_NAME"]
	}

	return res, err
}
