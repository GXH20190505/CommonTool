package dmbase

import (
	"database/sql"
	"fmt"
	"gitee.com/chunanyong/dm"
	"strings"
)

/* 数据库查询结果转换
把数据库中查询的Rows转换成DataTable格式，习惯操作
*/
type DataTable struct {
	Columns []string
	Count   int
	RowData []map[string]string //index=条数 key=Column value=value
}

/*  数据库操作
 */
type DMCache struct {
	ConnDB     *sql.DB
	ConStr     string
	DBAddr     string
	DBInstance string
	DBUser     string
	DBPwd      string
}

/** 新建达梦数据库缓存
"dm://SYSDBA:wanwei123@192.168.1.160:5236?BOOKSHOP&charset=utf8"
*/
func NewDMConnection(dbAddr string, dbInstance string, dbUser string, dbPwd string) (*DMCache, error) {
	var err error
	rs := &DMCache{
		ConnDB:     nil,
		ConStr:     fmt.Sprintf("dm://%s:%s@%s/%s&charset=utf8", dbUser, dbPwd, dbAddr, dbInstance),
		//ConStr: fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&CURRENT_SCHEMA=%s", dbUser, dbPwd, dbAddr, dbInstance,dbInstance),
		DBUser:     dbUser,
		DBPwd:      dbPwd,
		DBAddr:     dbAddr,
		DBInstance: dbInstance,
	}
	rs.ConnDB, err = sql.Open("dm", rs.ConStr)
	if err != nil {
		return nil, err
	}
	err = rs.ConnDB.Ping()
	if err != nil {
		return nil, err
	}
	// 设置默认 schema
	_, err = rs.ConnDB.Exec(fmt.Sprintf("ALTER SESSION SET CURRENT_SCHEMA = \"%s\"", dbInstance))
	if err != nil {
		return nil,err
	}
	rs.ConnDB.SetMaxIdleConns(50) //用于设置最大打开的连接数，默认值为0表示不限制
	rs.ConnDB.SetMaxOpenConns(50) //用于设置闲置的连接数
	return rs, err

}

func (self *DMCache) Ping() error {
	return self.ConnDB.Ping()
}
/** 数据库插入操作
 */
func (self *DMCache) ExecuteSql(strsql string) (sql.Result, error) {
	res, err := self.ConnDB.Exec(strsql)
	return res, err
}

/*数据库查询
 */
func (self *DMCache) SelectSql(strsql string) (DataTable, error) {
	//fmt.Println(strsql)
	var dt DataTable
	dt.RowData = make([]map[string]string, 0)
	rows, err := self.ConnDB.Query(strsql)
	//fmt.Println(rows)
	if err != nil {
		return dt, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	dt.Columns = columns
	//fmt.Println(columns)
	if err != nil {
		return dt, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	//fmt.Println(rows.Columns())
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
			columns[i] = strings.ToUpper(columns[i])
			tempTab[columns[i]] = value
		}

		dt.RowData = append(dt.RowData, tempTab)
		//fmt.Println(len(dt.rows))
		//fmt.Println(len(dt.rows[0]))
		//fmt.Println("************************")
	}
	dt.Count = len(dt.RowData)
	return dt, nil
}

//判断表名是否存在,返回true存在，false不存在,tabname表名，owner表所属者
func (self *DMCache) TabExist(tbname string) bool {
	//strSql:=fmt.Sprintf("SELECT COUNT(*) as count FROM ALL_TABLES WHERE OWNER='%s' AND TABLE_NAME = '%s';",self.DBInstance,tbname)
	strSql:=fmt.Sprintf("SELECT COUNT(*) as count FROM ALL_TABLES WHERE OWNER='%s' AND TABLE_NAME = '%s';",self.DBInstance,tbname)
	dt,err:= self.SelectSql(strSql)
	if err != nil {
		return false
	}
	if len(dt.RowData)>0{
		if dt.RowData[0]["COUNT"] == "0" {
			return false
		} else {
			return true
		}
	}else {
		return false
	}
}

/** 插入数据
返回值说明：sql语句，执行结果，错误信息
*/
func (self *DMCache) InsertData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	tabName = fmt.Sprintf("%s.%s",self.DBInstance, tabName)
	var strCol string
	var strVal string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for c, v := range HTCols { //把map组合成sql语句
		strCol += fmt.Sprintf("%s,", c)
		if strings.ToUpper(v) == "NULL" || v == "" { //
			strVal += fmt.Sprint("NULL,")
		} else {
			strVal += fmt.Sprintf("'%s',", v)
		}
	}

	if strCol == "" || strVal == "" {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	strsql := fmt.Sprintf("insert into %s(%s) values (%s)", tabName, strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}
//批量插入数据
func (self *DMCache) InsertDataSlice(tabName string, data []map[string]string) (string, sql.Result, error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	// 获取列名
	var columns []string
	for key := range data[0] {
		columns = append(columns, key)
	}

	// 动态生成列名和占位符
	colNames := strings.Join(columns, ", ")
	placeholders := strings.Trim(strings.Repeat("?,", len(columns)), ",")

	// 构建插入语句
	values := make([]interface{}, 0, len(data)*len(columns))
	valuePlaceholders := make([]string, 0, len(data))

	for _, record := range data {
		recordValues := make([]interface{}, len(columns))
		for i, col := range columns {
			val, ok := record[col]
			if !ok || val == "" {
				recordValues[i] = nil // 使用nil表示NULL
			} else {
				recordValues[i] = val
			}
		}
		values = append(values, recordValues...)
		valuePlaceholders = append(valuePlaceholders, placeholders)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tabName, colNames, strings.Join(valuePlaceholders, "),("))

	tx, err := self.ConnDB.Begin()
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return "", nil, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(values...)
	if err != nil {
		tx.Rollback()
		return "", nil, err
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return "", nil, err
	}
	return "successfulInserts", result, nil
}
//批量插入数据表，遇到重复插入则不处理继续插入
func  (self *DMCache)  InsertDataSliceIgnore(tabName string,data []map[string]string) (string, sql.Result, error) {
	if len(data) == 0 {
		return "", nil, nil // 如果没有数据，直接返回
	}
	// 动态生成列名和占位符
	var columns []string
	var placeholders []string
	var values []interface{}

	for key := range data[0] {
		columns = append(columns, key)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tabName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := self.ConnDB.Begin()
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return "",nil, err
	}
	defer stmt.Close()
	var successfulInserts strings.Builder

	for _, record := range data {
		values = nil // 清空values切片
		for _, col := range columns {
			values = append(values, record[col])
		}
		_, err := stmt.Exec(values...)
		if err != nil {
			if isUniqueConstraintError(err) {
				continue
			}
			tx.Rollback()
			return "",nil, err
		}
		// 记录成功的插入语句
		//insertStmt := buildInsertStatement(tabName,columns)
		//successfulInserts.WriteString(insertStmt)
	}
	// 提交事务
	if err := tx.Commit(); err != nil {
		return "",nil, err
	}
	return successfulInserts.String(),nil, nil
}
//增量更新单条数据
func (self *DMCache) DuplicateData(tabName string, data map[string]string) (string, sql.Result, error) {
	if len(data) == 0 {
		return "", nil, nil // 如果没有数据，直接返回
	}

	// 获取当前表的唯一索引
	uniqueColumns, err :=self.getUniqueIndexColumns(tabName)
	if err!= nil {
		return "", nil, err
	}

	// 动态生成列名和占位符
	var columns []string
	var placeholders []string
	var values []interface{}
	for key := range data {
		columns = append(columns, key)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tabName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := self.ConnDB.Begin()
	if err!= nil {
		return "", nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err!= nil {
		return "", nil, err
	}
	defer stmt.Close()

	var successfulInserts strings.Builder
	values = nil // 清空values切片
	for _, col := range columns {
		values = append(values, data[col])
	}
	_, err = stmt.Exec(values...)
	if err != nil {
		if isUniqueConstraintError(err) {
			// 当插入失败时候，根据唯一索引执行更新操作
			updateStmt, err := self.buildUpdateStatement(tabName, columns, uniqueColumns, data)
			if err != nil {
				tx.Rollback()
				return "", nil, err
			}
			_, err = tx.Exec(updateStmt)
			if err != nil {
				tx.Rollback()
				return "", nil, err
			}
		} else {
			tx.Rollback()
			return "", nil, err
		}
	} else {
		// 记录成功的插入语句
		//insertStmt := buildInsertStatement(tabName, columns)
		//successfulInserts.WriteString(insertStmt)
	}

	// 提交事务
	if err := tx.Commit(); err!= nil {
		return "", nil, err
	}

	return successfulInserts.String(), nil, nil
}
// 增量更新批量数据
func (self *DMCache) DupLicateDataSlice(tabName string, data []map[string]string) (string, sql.Result, error) {
	if len(data) == 0 {
		return "", nil, nil // 如果没有数据，直接返回
	}

	// 获取当前表的唯一索引
	uniqueColumns, err :=self.getUniqueIndexColumns(tabName)
	if err!= nil {
		return "", nil, err
	}

	// 动态生成列名和占位符
	var columns []string
	var placeholders []string
	var values []interface{}
	for key := range data[0] {
		columns = append(columns, key)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tabName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := self.ConnDB.Begin()
	if err!= nil {
		return "", nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err!= nil {
		return "", nil, err
	}
	defer stmt.Close()

	var successfulInserts strings.Builder
	for _, record := range data {
		values = nil // 清空values切片
		for _, col := range columns {
			values = append(values, record[col])
		}
		_, err := stmt.Exec(values...)
		if err!= nil {
			if isUniqueConstraintError(err) {
				// 当插入失败时候，根据唯一索引执行更新操作
				updateStmt, err := self.buildUpdateStatement(tabName, columns, uniqueColumns, record)
				if err!= nil {
					tx.Rollback()
					return "", nil, err
				}
				_, err = tx.Exec(updateStmt)
				if err!= nil {
					tx.Rollback()
					return "", nil, err
				}
			} else {
				tx.Rollback()
				return "", nil, err
			}
		} else {
			// 记录成功的插入语句
			insertStmt := buildInsertStatement(tabName, columns)
			successfulInserts.WriteString(insertStmt)
		}
	}

	// 提交事务
	if err := tx.Commit(); err!= nil {
		return "", nil, err
	}

	return successfulInserts.String(), nil, nil
}

// 获取表的唯一索引列名
func (self *DMCache) getUniqueIndexColumns(tableName string) ([]string, error) {
	// 这里假设查询达梦数据库中唯一索引列名的SQL语句如下，实际可能需要根据数据库版本和配置进行调整
	strSql := fmt.Sprintf("SELECT CON.CONSTRAINT_NAME, LISTAGG(COL.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY COL.POSITION) AS UNIQUE_COLUMNS FROM ALL_CONSTRAINTS CON JOIN  ALL_CONS_COLUMNS COL ON CON.CONSTRAINT_NAME = COL.CONSTRAINT_NAME AND CON.OWNER = COL.OWNER WHERE CON.CONSTRAINT_TYPE = 'U' AND CON.TABLE_NAME = '%s' GROUP BY CON.CONSTRAINT_NAME;", tableName)
	dt, err := self.SelectSql(strSql)
	if err!= nil {
		return nil, err
	}
	uniqueColumns := make([]string, dt.Count)
	for i := 0; i < dt.Count; i++ {
		uniqueColumns[i] = dt.RowData[i]["UNIQUE_COLUMNS"]
	}
	return uniqueColumns, nil
}
//判断报错如果是违反唯一约束导致，则继续插入，否则回滚
func isUniqueConstraintError(err error) bool {
	er:= err.(*dm.DmError)
	if er.ErrCode ==-6602 {
		return true
	}
	return false
}
// 构建插入语句
func buildInsertStatement(tabName string,columns []string) string {
	columnNames := strings.Join(columns, ", ")
	placeholderValues := strings.Repeat("?, ", len(columns)-1) + "?"
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",tabName, columnNames, placeholderValues)
}


// CreateColumns 创建表字段
func (self *DMCache) CreateColumns(tabName string, newCols map[string]string) (sql.Result, error) {
	if newCols == nil || len(newCols) == 0 {
		return nil, fmt.Errorf("添加空字段")
	}

	tabName = fmt.Sprintf("%s.%s", self.DBInstance, tabName)

	var strSql strings.Builder
	strSql.WriteString(fmt.Sprintf("ALTER TABLE %s ", tabName))

	for col, colType := range newCols {
		col = strings.Replace(col, "[", "", -1)
		col = strings.Replace(col, "]", "", -1)
		strSql.WriteString(fmt.Sprintf("ADD %s %s, ", col, colType))
	}

	// 移除最后一个逗号和空格
	query := strings.TrimSuffix(strSql.String(), ", ")

	//fmt.Println(query) // 用于调试，可以删除或注释掉

	return self.ExecuteSql(query)
}
///////////////////////////////////未验证//////////////////////////////////////////////////////////
func  (self *DMCache)  DupLIcateSliceDatabase(tabName string,data []map[string]string) (string, sql.Result, error) {
	if len(data) == 0 {
		return "", nil, nil // 如果没有数据，直接返回
	}
	//获取当前表的唯一索引
	// 动态生成列名和占位符
	var columns []string
	var placeholders []string
	var values []interface{}

	for key := range data[0] {
		columns = append(columns, key)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tabName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := self.ConnDB.Begin()
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return "",nil, err
	}
	defer stmt.Close()
	var successfulInserts strings.Builder
	for _, record := range data {
		values = nil // 清空values切片
		for _, col := range columns {
			values = append(values, record[col])
		}
		_, err := stmt.Exec(values...)
		if err != nil {
			if isUniqueConstraintError(err) {
				//当插入失败时候，获取表唯一索引执行更新操作

			}
			tx.Rollback()
			return "",nil, err
		}
		// 记录成功的插入语句
		insertStmt := buildInsertStatement(tabName,columns)
		successfulInserts.WriteString(insertStmt)
	}
	// 提交事务
	if err := tx.Commit(); err != nil {
		return "",nil, err
	}
	return successfulInserts.String(),nil, nil
}

// buildUpdateStatement 构建更新语句
func (self *DMCache) buildUpdateStatement(tabName string, allColumns []string, uniqueColumns []string, record map[string]string) (string, error) {
	setValues := make([]string, 0)
	for _, col := range allColumns {
		value := record[col]
		if value == "" {
			setValues = append(setValues, fmt.Sprintf("%s = NULL", col))
		} else {
			// 假设这里对单引号进行简单转义，实际应用中可能需要更完善的处理
			value = strings.ReplaceAll(value, "'", "''")
			setValues = append(setValues, fmt.Sprintf("%s = '%s'", col, value))
		}
	}

	matchCondition :=strings.Builder{}
	for _, col := range uniqueColumns {
		splitUnique := strings.Split(col, ",")
		for _, s := range splitUnique {
			s=strings.Trim(s," ")
			if matchCondition.String()!= "" {
				matchCondition.WriteString(" AND ")
			}
			if record == nil || record[s] == "" {
				continue // 跳过无效的记录
			}
			matchCondition.WriteString(fmt.Sprintf("%s = '%s'", s, record[s]))
		}

	}

	updateSql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tabName, strings.Join(setValues, ", "), matchCondition.String())
	return updateSql, nil
}

// DuplicateData根据唯一索引判断重复并执行插入或更新操作
func (self *DMCache) DuplicateData2(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	var strCol string
	var strVal string
	var strUp string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}


	// 构建插入列和值的部分
	for c, v := range HTCols {
		strCol += fmt.Sprintf("%s,", c)
		if v == "" {
			strVal += fmt.Sprint("NULL,")
			strUp += fmt.Sprintf("%s=NULL,", c)
		} else {
			// 假设这里对单引号进行简单转义，实际应用中可能需要更完善的处理
			v = strings.ReplaceAll(v, "'", "''")
			strVal += fmt.Sprintf("'%s',", v)
			strUp += fmt.Sprintf("%s='%s',", c, v)
		}
	}
	if strCol == "" || strVal == "" {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	uniqueColumns, err := self.getUniqueIndexColumns(tabName)
	if err!= nil {
		return "", nil, err
	}
	if len(uniqueColumns) == 0 {
		return "", nil, fmt.Errorf("表 %s 没有唯一索引", tabName)
	}

	// 构建MERGE INTO语句的匹配条件
	matchCondition := ""
	for _, col := range uniqueColumns {
		if matchCondition!= "" {
			matchCondition += " AND "
		}
		matchCondition += fmt.Sprintf("t.%s = s.%s", col, col)
	}
	// 构建MERGE INTO语句
	strsql := fmt.Sprintf("MERGE INTO %s t USING (SELECT * FROM  cj_dtgk_data_2011_20240909)s ON (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
		tabName,
		matchCondition,
		strUp[:len(strUp)-1],
		strCol[:len(strCol)-1],
		strVal[:len(strVal)-1])
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}
// GetColumns获取表字段
func (self *DMCache) GetColumns(tableName string) string {
	// 达梦数据库获取表字段的SQL语句示例，需根据实际情况调整
	strSql := fmt.Sprintf("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '%s'", tableName)
	dt, err := self.SelectSql(strSql)
	if err!= nil {
		return ""
	}
	var cols string
	for i := 0; i < dt.Count; i++ {
		cols += fmt.Sprintf("[%s]", dt.RowData[i]["COLUMN_NAME"])
	}
	return cols
}
func (self *DMCache) DuplicateData3(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	var strCol []string
	var strVal []string
	var strUp []string

	for c, v := range HTCols {
		strCol = append(strCol, c)
		if strings.ToUpper(v) == "NULL" || v == "" {
			strVal = append(strVal, "NULL")
			strUp = append(strUp, fmt.Sprintf("%s = NULL", c))
		} else {
			strVal = append(strVal, fmt.Sprintf("'%s'", v))
			strUp = append(strUp, fmt.Sprintf("%s = '%s'", c, v))
		}
	}

	if len(strCol) == 0 || len(strVal) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	colStr := strings.Join(strCol, ", ")
	valStr := strings.Join(strVal, ", ")
	upStr := strings.Join(strUp, ", ")

	// 构建 MERGE 语句
	strsql := fmt.Sprintf(`
		MERGE INTO %s AS target
		USING (SELECT %s FROM DUAL) AS source (%s)
		ON (%s)
		WHEN MATCHED THEN
			UPDATE SET %s
		WHEN NOT MATCHED THEN
			INSERT (%s) VALUES (%s)
	`, tabName, colStr, colStr, colStr, upStr, colStr, valStr)

	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}