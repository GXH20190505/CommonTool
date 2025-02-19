package linuxmssqlbase

import (
	"database/sql"
	"fmt"
	"strings"
	"strconv"
	_ "github.com/denisenkom/go-mssqldb"
)
type DataTable struct {
	Columns []string
	Count   int
	RowData []map[string]string //index=条数 key=Column value=value
}

type MssqlCache struct {
	connDB     *sql.DB
	ConStr     string
	DBAddr     string
	DBInstance string
	DBUser     string
	DBPwd      string
}
/*新建mssql数据库连接缓存
 */
func NewMSSQLConnection(dbAddr string, dbInstance string, dbUser string, dbPwd string) (*MssqlCache, error) {
	var err error
	//sql.Open("odbc", "driver={SQL Server};Server=192.168.0.250;Database=tjlhYC20;uid=sa;pwd=sql2008!@#;")
	rs := &MssqlCache{
		connDB:     nil,
		ConStr:     fmt.Sprintf("server=%s;user id=%s;password=%s;port=1433;database=%s;encrypt=disable", dbAddr, dbUser, dbPwd,dbInstance),
		DBUser:     dbUser,
		DBPwd:      dbPwd,
		DBAddr:     dbAddr,
		DBInstance: dbInstance,
	}
	rs.connDB, err = sql.Open("mssql", rs.ConStr)
	err = rs.connDB.Ping()
	rs.connDB.SetMaxIdleConns(50)  //用于设置最大打开的连接数，默认值为0表示不限制
	rs.connDB.SetMaxOpenConns(100) //用于设置闲置的连接数
	return rs, err

}
/*判断表是否存在
返回值：false =表不存在 true=表存在
*/
func (self *MssqlCache) TabExist(tableName string) bool {
	strSql := fmt.Sprintf("select count(*) as COUNT from sysobjects where id = object_id(N'[%s]') and OBJECTPROPERTY(id, N'IsUserTable') = 1",
		tableName)
	//fmt.Println(strSql)
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
/*数据库查询
 */
func (self *MssqlCache) SelectSql(strsql string) (DataTable, error) {
	//fmt.Println(strsql)
	var dt DataTable
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
func Conn() {
	ConnCache,err:= NewMSSQLConnection("192.168.100.23","yyzf","sa","sql2008!@#")
	if err!=nil{
		fmt.Printf(" connString失败")
	}
	fmt.Println("")
	value:=make(map[string]string)
	value["NAME"]="郭晓鹤"
	where:=make(map[string]string)
	where["ID"]="1"
	ConnCache.UpdateOrInsert("test",value,where)
	//connString := "driver={SQL Server};Server=192.168.100.23;user id=sa;pwd=sql2008!@#;encrypt=disable "// fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", *server, *user, *password, *port)
	/*connString :="server=192.168.100.23;user id=sa;password=sql2008!@#;port=1433;database=yyzf;encrypt=disable"
		fmt.Printf(" connString:%s\n", connString)

	conn, err := sql.Open("mssql", connString)
	if err != nil {
		fmt.Println("Open connection failed:", err.Error())
	}
	fmt.Println("Open connection succ")
/*	err=conn.Ping()
	if err != nil {
		fmt.Println("ping connection failed:", err.Error())
	}
	fmt.Println("ping connection succ")*/
	/*conn.SetMaxOpenConns(50)
	conn.SetMaxOpenConns(100)

	defer conn.Close()
	stmt, err := conn.Prepare("update test set Name='王成林' where ID=1 ")
	if err != nil {
		log.Fatal("Prepare failed:", err.Error())
	}

	defer conn.Close()
	row,err := stmt.Exec()
	if err != nil {
		log.Fatal("query failed:", err.Error())
	}
	fmt.Println(row.RowsAffected())
	var somenumber int64
	var somechars string
	//err = row.Scan(&somenumber, &somechars)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}

	fmt.Printf("somenumber:%d\n", somenumber)
	fmt.Printf("somechars:%s\n", somechars)

	fmt.Printf("bye\n")*/
}
func(self *MssqlCache) UpdateOrInsert(tabName string, HTCols map[string]string, HTWhere map[string]string) (string, error) {
	var strsql string
	var strWhere string
	for c, v := range HTCols {
		if strsql != "" {
			strsql += ","
		}
		if strings.ToUpper(v) == "NULL" || v == "" {
			strsql += fmt.Sprintf("%s=NULL", c)
		} else {
			strsql += fmt.Sprintf("%s='%s'", c, v)
		}
	}
	if HTWhere != nil {
		for c, v := range HTWhere {
			if strWhere != "" {
				strWhere += " AND "
			}
			strWhere += fmt.Sprintf("%s='%s'", c, v)

		}
	}
	if strWhere != "" {
		strsql = fmt.Sprintf("UPDATE %s SET %s WHERE %s", tabName, strsql, strWhere)
	} else {
		strsql = fmt.Sprintf("UPDATE %s SET %s", tabName, strsql, strWhere)
	}
	i, err :=self.ExecuteSql(strsql)
	if err != nil {
		return strsql, err
	}
	/*
		fmt.Println("------------------------")
		fmt.Println(strsql)
		fmt.Println(i.LastInsertId())
		fmt.Println(i.RowsAffected())
		fmt.Println("------------------------")*/

	updateRows, _ := i.RowsAffected()
	if updateRows == 0 {
		strsql, _, err = self.InsertData(tabName, HTCols)
	}
	return strsql, err
}
/*执行数据库插入等操作
*/
func(self *MssqlCache) ExecuteSql(strSql string) (sql.Result, error) {
	//fmt.Println(strSql)
	res, err := self.connDB.Exec(strSql)
	return res, err

}
func (self *MssqlCache) InsertData(tabName string, HTCols map[string]string) (string, sql.Result, error) {
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

	strsql := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)", tabName, strCol[:len(strCol)-1], strVal[:len(strVal)-1])
	r, err := self.ExecuteSql(strsql)
	return strsql, r, err
}
/** 获取表字段
返回值：所有表字段的字符串，格式：[field1][field2][...]
*/
func (self *MssqlCache) GetColumns(tableName string) string {
	strSql := fmt.Sprintf("select NAME from syscolumns where id=object_id('%s')", tableName)
	dt, err := self.SelectSql(strSql)
	if err != nil {
		return ""
	}
	var cols string
	for i := 0; i < dt.Count; i++ {
		cols += fmt.Sprintf("[%s]", dt.RowData[i]["NAME"])
	}
	return cols
}
/** 获取所有表字段
返回值说明 ： key= tablebname value =[field1][field2][...]
*/
func (self *MssqlCache) GetColumnsAll() map[string]string {
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
func (self *MssqlCache) CreateColumns(tabName string, newCols map[string]string) (string, error) {
	if newCols == nil {
		return "", fmt.Errorf("添加空字段")
	}
	var strSql string
	strSql = fmt.Sprintf("ALTER TABLE %s add ", tabName)
	for col, colType := range newCols {
		strSql += fmt.Sprintf("%s %s,", col, colType)
	}
	strSql = strSql[:len(strSql)-1]
	_, err := self.ExecuteSql(strSql)
	return strSql, err
}

