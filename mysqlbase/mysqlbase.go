//MySQL基础层
//主要用于与MySQL连接
package mysqlbase

import (
	"database/sql"
	"encoding/json"
	//"errorlog"
	"fmt"
	"strconv"
	"strings"
	"timebase"

	_ "github.com/go-sql-driver/mysql"
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
type MysqlCache struct {
	connDB     *sql.DB
	ConStr     string
	DBAddr     string
	DBInstance string
	DBUser     string
	DBPwd      string
}

/* 新建MySQL数据库连接缓存
 */
func NewMySQLConnection(dbAddr string, dbInstance string, dbUser string, dbPwd string) (*MysqlCache, error) {
	var err error
	rs := &MysqlCache{
		connDB:     nil,
		ConStr:     fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPwd, dbAddr, dbInstance),
		DBUser:     dbUser,
		DBPwd:      dbPwd,
		DBAddr:     dbAddr,
		DBInstance: dbInstance,
	}
	rs.connDB, err = sql.Open("mysql", rs.ConStr)
	err = rs.connDB.Ping()
	rs.connDB.SetMaxIdleConns(50) //用于设置最大打开的连接数，默认值为0表示不限制
	rs.connDB.SetMaxOpenConns(50) //用于设置闲置的连接数
	//
	//rs.connDB.SetConnMaxLifetime(50)
	return rs, err

}

func (self *MysqlCache) Ping() error {
	return self.connDB.Ping()
}

/*数据库插入等操作
 */
func (self *MysqlCache) ExecuteSql(strSql string) (sql.Result, error) {
	//fmt.Println(strSql)
	res, err := self.connDB.Exec(strSql)
	return res, err
}
func (self *MysqlCache) ExecuteSql2(strSql,data string) (sql.Result, error) {
	//fmt.Println(strSql)
	res, err := self.connDB.Exec(strSql,data)
	return res, err
}
/*开启事务执行sql语句
 */
func (self *MysqlCache) ExecuteTransactionSqls(strSqls ...string) (sql.Result, error) {
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

/*数据库查询
 */
func (self *MysqlCache) SelectSql(strsql string) (DataTable, error) {
	//fmt.Println(strsql)
	var dt DataTable
	dt.RowData = make([]map[string]string, 0)
	rows, err := self.connDB.Query(strsql)
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

/** 判断表是否存在
返回值： false=表不存在，true=表存在
*/

func (self *MysqlCache) TabExist(tableName string) bool {
	strSql := fmt.Sprintf("select count(*) as count from information_schema.tables where table_name='%s' and table_schema='%s'",
		tableName, self.DBInstance)
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

/** 获取表字段
返回值：所有表字段的字符串，格式：[field1][field2][...]
*/
func (self *MysqlCache) GetColumns(tableName string) string {
	strSql := fmt.Sprintf("show fields from %s", tableName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return ""
	}
	var cols string
	for i := 0; i < dt.Count; i++ {
		cols += fmt.Sprintf("[%s]", dt.RowData[i]["FIELD"])
	}
	return cols
}

func (self *MysqlCache) GetColumnList(tabName string) []string {

	var cols []string
	strSql := fmt.Sprintf("show fields from %s", tabName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return cols
	}
	for i := 0; i < dt.Count; i++ {
		cols = append(cols, dt.RowData[i]["FIELD"])
	}
	return cols
}

/** 获取所有表字段
返回值说明 ： key= tablebname value =[field1][field2][...]
注：查询的表名、列名都是大写
*/
func (self *MysqlCache) GetColumnsAll() map[string]string {
	strSql := fmt.Sprintf("select COLUMN_NAME ,TABLE_NAME from information_schema.columns where table_schema='%s'", self.DBInstance)
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

/** 创建表字段
 */
func (self *MysqlCache) CreateColumns(tabName string, newCols map[string]string) (sql.Result, error) {
	if newCols == nil {
		return nil, fmt.Errorf("添加空字段")
	}
	var strSql string
	strSql = fmt.Sprintf("alter table %s ", tabName)
	for col, colType := range newCols {
		col = strings.Replace(col, "[", "", -1)
		col = strings.Replace(col, "]", "", -1)
		strSql += fmt.Sprintf("add `%s` %s,", col, colType)
	}
	strSql = strSql[:len(strSql)-1]
	//fmt.Println(strSql)
	return self.ExecuteSql(strSql)
}

/** 插入数据
返回值说明：sql语句，执行结果，错误信息
*/
func (self *MysqlCache) InsertData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
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
func (self *MysqlCache) InsertSliceData(tabName string, data []map[string]string) (string, sql.Result, error) {
	mpCol := make(map[string]string)
	vals := make([]string, 0)
	var strUp string
	cols := make([]string, 0)
	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		//strVal := "("
		for c, _ := range HTCols { //把map组合成sql语句
			mpCol[c] = ""
			//if strings.ToUpper(v) == "NULL" || v == "" { //
			//	strVal += fmt.Sprint("NULL,")
			//} else {
			//	strVal += fmt.Sprintf("'%s',", v)
			//}
		}
		//vals = append(vals, strVal+")")
	}
	for col, _ := range mpCol {
		cols = append(cols, fmt.Sprintf("`%s`",col))
		strUp += fmt.Sprintf("`%s`=VALUES(`%s`),", col, col)
	}
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		strVal := "("
		for _, col := range cols {
			key:=strings.ReplaceAll(col,"`","")
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

	strsql := fmt.Sprintf("insert into %s(%s) values %s ", tabName, strings.Join(cols, ","), strings.Join(vals, ","))
	r, err := self.ExecuteSql(strsql)
	//errorlog.ErrorLogDebug("test_sql", "sql", strsql)
	return strsql, r, err
}
/*
若出现重复数据不进行更新操作
*/
func (self *MysqlCache) InsertNoUpdateDuplicate(tabName string, data []map[string]string) (string, sql.Result, error) {
    mpCol := make(map[string]string)
    vals := make([]string, 0)
    cols := make([]string, 0)

    if len(data) == 0 {
        return "", nil, fmt.Errorf("没有数据插入")
    }

    // 生成所有列名
    for _, HTCols := range data {
        for c := range HTCols {
            mpCol[c] = ""
        }
    }

    // 生成列名的列表，并保留反引号
    for col := range mpCol {
        cols = append(cols, fmt.Sprintf("`%s`", col))
    }

    if len(cols) == 0 {
        return "", nil, fmt.Errorf("没有数据插入")
    }

    // 构建每一行的值
    for _, HTCols := range data {
        strVal := "("

        for _, col := range cols {
            key := strings.ReplaceAll(col, "`", "")
            if HTCols[key] == "" {
                strVal += "NULL,"
            } else {
                escapedValue := strings.ReplaceAll(HTCols[key], "'", "''")  // 转义单引号
                strVal += fmt.Sprintf("'%s',", escapedValue)
                //strVal += fmt.Sprintf("%s,", escapedValue)
            }
        }

        // 去掉最后一个逗号，并加上右括号
        strVal = strVal[:len(strVal)-1] + ")"
        vals = append(vals, strVal)
    }

    if len(vals) == 0 {
        return "", nil, fmt.Errorf("没有数据插入")
    }

    // 生成最终的插入 SQL
    strsql := fmt.Sprintf("INSERT IGNORE INTO %s(%s) VALUES %s", tabName, strings.Join(cols, ","), strings.Join(vals, ","))

    // 执行 SQL
    r, err := self.ExecuteSql(strsql)
    return strsql, r, err
}
func isValidJSON(s string) bool {
	var js map[string]interface{}
	err := json.Unmarshal([]byte(s), &js)
	return err == nil
}

func (self *MysqlCache) InsertNoUpdateDuplicatebase(tabName string, data []map[string]string) (string, sql.Result, error) {
	mpCol := make(map[string]string)
	vals := make([]string, 0)
	cols := make([]string, 0)

	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for _, HTCols := range data {
		for c, _ := range HTCols {
			mpCol[c] = ""
		}
	}

	for col, _ := range mpCol {
		cols = append(cols, fmt.Sprintf("`%s`", col))
	}

	if len(cols) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for _, HTCols := range data {
		strVal := "("

		for _, col := range cols {
			key := strings.ReplaceAll(col, "`", "")
			if HTCols[key] == "" {
				strVal += fmt.Sprint("NULL,")
			} else {
				strVal += fmt.Sprintf("'%s',", HTCols[key])
			}
		}

		strVal = strVal[:len(strVal)-1] + ")"
		vals = append(vals, strVal)
	}

	if len(vals) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	strsql := fmt.Sprintf("INSERT IGNORE INTO %s(%s) VALUES %s", tabName, strings.Join(cols, ","), strings.Join(vals, ","))

	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}
/** 插入数据
返回值说明：sql语句，执行结果，错误信息
*/
func (self *MysqlCache) ReplaceData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
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
	strsql := fmt.Sprintf("replace into %s(%s) values (%s)", tabName, strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}

/** 更新数据
返回值说明：sql语句，执行结果，错误信息
说明：使用的表需要有唯一索引
如果出现重复数据，则更新，否则插入
*/
func (self *MysqlCache) DuplicateData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
	var strCol string
	var strVal string
	var strUp string
	if HTCols == nil {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for c, v := range HTCols { //把map组合成sql语句
		strCol += fmt.Sprintf("%s,", c)
		if strings.ToUpper(v) == "NULL" || v == "" { //
			strVal += fmt.Sprint("NULL,")
			strUp += fmt.Sprintf("`%s`=NULL,", c)
		} else {
			strVal += fmt.Sprintf("'%s',", v)
			strUp += fmt.Sprintf("%s='%s',", c, v)
		}
	}
	if strCol == "" || strVal == "" {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	strsql := fmt.Sprintf("insert into %s(%s) values (%s)  ON DUPLICATE KEY UPDATE %s", tabName, strCol[:len(strCol)-1], strVal[:len(strVal)-1], strUp[:len(strUp)-1])
	r, err := self.ExecuteSql(strsql)
	//errorlog.ErrorLogDebug("test_sql", "sql", strsql)
	return strsql, r, err
}

func (self *MysqlCache) UpdateDataOne(tabName string, htCol map[string]string, htWhere map[string]string) (string, error) {
	defer func() {
		if err := recover(); err != nil {
			//return err
		}
	}()
	var strCol []string
	var strWhere []string
	for k, v := range htCol {
		if strings.ToUpper(v) == "NULL" || v == "" {
			strCol = append(strCol, fmt.Sprintf("%s=NULL", k))
		} else {
			strCol = append(strCol, fmt.Sprintf("%s='%s'", k, v))
		}
	}

	for k, v := range htWhere {
		strWhere = append(strWhere, fmt.Sprintf("%s='%s'", k, v))
	}

	strsql := fmt.Sprintf("update %s set %s where %s", tabName, strings.Join(strCol, ","), strings.Join(strWhere, " and "))
	_, err := self.ExecuteSql(strsql)
	return strsql, err
}

/** 批量新增和更新数据
返回值说明：sql语句，执行结果，错误信息
说明：使用的表需要有唯一索引
如果出现重复数据，则更新，否则插入
*/
func (self *MysqlCache) DuplicateSliceData(tabName string, data []map[string]string) (string, sql.Result, error) {
	mpCol := make(map[string]string)
	vals := make([]string, 0)
	var strUp string
	cols := make([]string, 0)
	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		//strVal := "("
		for c, _ := range HTCols { //把map组合成sql语句
			mpCol[c] = ""
			//if strings.ToUpper(v) == "NULL" || v == "" { //
			//	strVal += fmt.Sprint("NULL,")
			//} else {
			//	strVal += fmt.Sprintf("'%s',", v)
			//}
		}
		//vals = append(vals, strVal+")")
	}
	for col, _ := range mpCol {
		cols = append(cols, fmt.Sprintf("`%s`",col))
		strUp += fmt.Sprintf("`%s`=VALUES(`%s`),", col, col)
	}
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		strVal := "("
		for _, col := range cols {
			key:=strings.ReplaceAll(col,"`","")
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

	strsql := fmt.Sprintf("insert into %s(%s) values %s  ON DUPLICATE KEY UPDATE %s", tabName, strings.Join(cols, ","), strings.Join(vals, ","), strUp[:len(strUp)-1])
	r, err := self.ExecuteSql(strsql)
	//errorlog.ErrorLogDebug("test_sql", "sql", strsql)
	return strsql, r, err
}
func (self *MysqlCache) DuplicateSliceData2(tabName string, data []map[string]string) (string, sql.Result, error) {
	mpCol := make(map[string]string)
	vals := make([]string, 0)
	var strUp string
	cols := make([]string, 0)
	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		//strVal := "("
		for c, _ := range HTCols { //把map组合成sql语句
			mpCol[c] = ""
			//if strings.ToUpper(v) == "NULL" || v == "" { //
			//	strVal += fmt.Sprint("NULL,")
			//} else {
			//	strVal += fmt.Sprintf("'%s',", v)
			//}
		}
		//vals = append(vals, strVal+")")
	}
	for col, _ := range mpCol {
		cols = append(cols, col)
		strUp += fmt.Sprintf("`%s`=IFNULL(VALUES(`%s`), `%s`),", col, col,col)
	}
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}
	for _, HTCols := range data {
		strVal := "("
		for _, col := range cols {
			if HTCols[col] == "" {
				strVal += fmt.Sprint("NULL,")
			} else {
				strVal += fmt.Sprintf("'%s',", HTCols[col])
			}
		}
		vals = append(vals, strVal[:len(strVal)-1]+")")
	}

	if len(vals) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	strsql := fmt.Sprintf("insert into %s(%s) values %s  ON DUPLICATE KEY UPDATE %s", tabName, strings.Join(cols, ","), strings.Join(vals, ","), strUp[:len(strUp)-1])
	r, err := self.ExecuteSql(strsql)
	//errorlog.ErrorLogDebug("test_sql", "sql", strsql)
	return strsql, r, err
}

/** 查询数据条数
返回查询条数
tabName = 表名
strWhere = 查询条件
返回值
-1 = 表示查询失败
0~N = 表示查询结果
*/
func (self *MysqlCache) GetCount(tabName string, strWhere string) int {
	strsql := fmt.Sprintf("select COUNT(*) as COUNT from %s where %s", tabName, strWhere)
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

/*数据库批量插入等操作
slice sql语句的集合
在一个连接下执行多条sql语句
返回执行错误的sql语句，和错误原因
*/
func (self *MysqlCache) BatchExecuteSql(slice []string) (string, error) {
	//fmt.Println(strSql)
	var err error
	j := 0
	tx, _ := self.connDB.Begin()
	if slice != nil {
		for i := range slice {
			if slice[i] == "" {
				continue
			}
			_, err = tx.Exec(slice[i])
			if err != nil {
				j = i
				break
			}

		}
	}
	tx.Commit()
	return slice[j], err

}

/*判断是否为合法数字且不为0*/
func GetInteger(value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("转换数据为空")
	}
	return strconv.Atoi(value)
}

/*
func (self *mysqlCache)SelectSql(strSql string)(*sql.Rows, error){
	Rows,err:=self.connDB.Query(strSql)
	if err!=nil{
		errorlog.ErrorLogWarn("SQL","数据库查询失败",strSql+"\r\n"+err.Error());
		return Rows,err
	}
	return Rows,nil
}
*/

/** 统计型字符串
传入map,传出string
如 mp["SO2"]="SUM" => SUM(SO2) as SO2,
*/
func ReportString(param map[string]string) string {
	if param == nil {
		return ""
	}
	sqls := ""
	for k, v := range param {
		sqls = fmt.Sprintf("%s(%s) as %s,%s", v, k, k, sqls)
	}
	if len(sqls) > 5 {
		return sqls[:len(sqls)-1]
	}
	return ""
}

/** 数据转换成插入sql
tabName =表名
dt = 插入数据，其中dt.Columns需要有数据，dt.RowData需要有数据
priKey = 自增ID，不用写入插入SQL
[]map[string]string
*/
func DataToString(tabName string, dt DataTable, priKey string) string {
	cols := []string{}
	priKey = strings.ToUpper(priKey)
	for _, col := range dt.Columns {
		col = strings.ToUpper(col)
		if col != priKey {
			cols = append(cols, col)
		}
	}
	inStr := strings.Join(cols, ",")
	valStr := []string{}
	for _, rows := range dt.RowData {
		val := []string{}
		for _, col := range cols {
			if col == priKey {
				continue
			}
			v := rows[col]
			if strings.ToUpper(v) == "NULL" || v == "" { //
				val = append(val, "NULL")
			} else {
				val = append(val, fmt.Sprintf("'%s'", v))
			}
		}

		valStr = append(valStr, fmt.Sprintf("(%s)", strings.Join(val, ",")))
	}
	return fmt.Sprintf("insert into %s(%s) VALUES %s", tabName, inStr, strings.Join(valStr, ","))
}

/** 数据转换成sql
min~max：
*/
func DataToStringByPage(tabName string, dt DataTable, priKey string, min int, max int) string {
	cols := []string{}
	for _, col := range dt.Columns {
		if strings.ToUpper(col) != strings.ToUpper(priKey) {
			cols = append(cols, col)
		}
	}
	inStr := strings.Join(cols, ",")
	valStr := []string{}
	for i := min; i < max; i++ {
		rows := dt.RowData[i]
		val := []string{}
		for _, col := range cols {
			if strings.ToUpper(col) == strings.ToUpper(priKey) {
				continue
			}
			v := rows[col]
			if strings.ToUpper(v) == "NULL" || v == "" { //
				val = append(val, "NULL")
			} else {
				val = append(val, fmt.Sprintf("'%s'", v))
			}
		}

		valStr = append(valStr, fmt.Sprintf("(%s)", strings.Join(val, ",")))
	}
	return fmt.Sprintf("insert into %s(%s) VALUES%s", tabName, inStr, strings.Join(valStr, ","))
}

func (self *MysqlCache) InsertNoUpdateDuplicateSlice(tabName string, data []map[string]string) (string, sql.Result, error) {
	mpCol := make(map[string]string)
	vals := make([]string, 0)
	cols := make([]string, 0)

	if len(data) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for _, HTCols := range data {
		for c, _ := range HTCols {
			mpCol[c] = ""
		}
	}

	for col, _ := range mpCol {
		cols = append(cols, fmt.Sprintf("`%s`", col))
	}

	if len(cols) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	for _, HTCols := range data {
		strVal := "("

		for _, col := range cols {
			key := strings.ReplaceAll(col, "`", "")
			if strings.ToUpper(key) == "STORAGE_DT" {
				HTCols[key] = timebase.NowTimeFormatMillisecond()
			}
			if HTCols[key] == "" {
				strVal += fmt.Sprint("NULL,")
			} else {
				strVal += fmt.Sprintf("'%s',", HTCols[key])
			}
		}

		strVal = strVal[:len(strVal)-1] + ")"
		vals = append(vals, strVal)
	}

	if len(vals) == 0 {
		return "", nil, fmt.Errorf("没有数据插入")
	}

	strsql := fmt.Sprintf("INSERT IGNORE INTO %s(%s) VALUES %s", tabName, strings.Join(cols, ","), strings.Join(vals, ","))

	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}