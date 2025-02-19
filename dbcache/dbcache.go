package dbcache

import (
	"errorlog"
	"fmt"
	"mysqlbase"
	"reflect"
	"stringbase"
	"strings"
	"time"
)

/** 表缓存机制
 */
const (
	MAX_CACHE = 500
)

type DBCache struct {
	MysqlDB   *mysqlbase.MysqlCache //数据库连接
	TableName string                //表名
	Columns   []string              //字段列表
	Datacache chan interface{}      //数据缓存,先测试json

	colValue  string //insert into (%s)  %s=colValue
	colUpdate string //on duplicate %s   %s=colUpdate
}

/** 外部接口 -- 创建数据表缓存对象
mysqlDB:传入数据库连接
name:传入表名
cols:
	1、可传入json结构
	2、可传入map
	3、传入prikey（自增字段名，大写）
*/
func NewCache(mysqlDB *mysqlbase.MysqlCache, tabname string, cols interface{}) *DBCache {
	db := new(DBCache)
	db.TableName = tabname
	db.MysqlDB = mysqlDB

	db.Datacache = make(chan interface{}, MAX_CACHE)
	//初始化列名
	v := reflect.ValueOf(cols)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map:
		db.InitColumnbyJson(cols)

	default:
		db.InitColumnbyMysql(fmt.Sprintf("%s", cols))
	}
	errorlog.ErrorLogInfo("dbcache", db.TableName, stringbase.JsonToString(db.Columns)+"\r\n"+db.colValue+"\r\n"+db.colUpdate)

	go db.Start()
	return db
}

/** 定期执行
 */
func (this *DBCache) Start() {
	//判断队列长度
	var dataCachan []interface{}
	for {
		select {
		case data := <-this.Datacache:
			//fmt.Println("append data")
			dataCachan = append(dataCachan, data)
			if len(dataCachan) >= 1000 {
				fmt.Println(this.TableName, "data cache insert:", len(dataCachan))
				this.insert_data(dataCachan)
				dataCachan = make([]interface{}, 0)
			}

		case <-time.After(5 * time.Second):
			fmt.Println(this.TableName, "data cache wait time out")
			if len(dataCachan) > 0 {
				this.insert_data(dataCachan)
				dataCachan = make([]interface{}, 0)
			}
		}
		//fmt.Println("exit select")
	}
}

/** 根据json创建列名
 */
func (this *DBCache) InitColumnbyJson(js interface{}) {
	this.Columns = parseTag(js)
	this.initColumnsNext()
}

/** 根据map创建列名
 */
func (this *DBCache) InitColumnbyMap(colmap map[string]string) {
	this.Columns = []string{}
	for _, col := range colmap {
		this.Columns = append(this.Columns, col)
	}
	this.initColumnsNext()
}

/** 从数据库中查询列名
priKey=自增ID
*/
func (this *DBCache) InitColumnbyMysql(priKey string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("slice err:", err)
		}
	}()
	this.Columns = this.MysqlDB.GetColumnList(this.TableName)
	for i := 0; i < len(this.Columns); i++ {
		if this.Columns[i] == priKey {
			this.Columns = append(this.Columns[:i], this.Columns[i+1:]...)
		}
	}
	this.initColumnsNext()
}

/** 初始化列名后，需要更新对应的入库和更新字段
 */
func (this *DBCache) initColumnsNext() {
	upVals := []string{}
	for _, col := range this.Columns {
		upVals = append(upVals, fmt.Sprintf("`%s`=VALUES(`%s`)", col, col))
	}
	this.colValue = strings.Join(this.Columns, ",")
	this.colUpdate = strings.Join(upVals, ",")
}

/** 外部接口 -- 写入json
 */
func (this *DBCache) AppendData(js interface{}) {
	//if len(this.Datacache) < MAX_CACHE {
	this.Datacache <- js
	//}
}

/** 写入数据
 */
func (this *DBCache) insert_data(data []interface{}) {
	if len(data) < 1 {
		return
	}
	vals := []string{}
	for _, param := range data {
		jsVal := parseVal(param)

		val := []string{}
		for _, col := range this.Columns {
			v := jsVal[col]
			//fmt.Println("jsval1", col, v, jsVal)
			//fmt.Println("jsval2", col, v, this.Columns)
			if strings.ToUpper(v) == "NULL" || v == "" {
				val = append(val, "NULL")
			} else {
				val = append(val, fmt.Sprintf("'%s'", v))
			}
		}
		vals = append(vals, fmt.Sprintf("(%s)", strings.Join(val, ",")))
	}
	strsql := fmt.Sprintf("insert into %s(%s) VALUES %s ON DUPLICATE KEY UPDATE %s", this.TableName, this.colValue, strings.Join(vals, ","), this.colUpdate)
	_, err := this.MysqlDB.ExecuteSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL_BATCH", this.TableName, fmt.Sprintf("%v\r\n%s", err, strsql))
	}
	fmt.Println(strsql)
	errorlog.ErrorLogInfo("BATCH_RESULT", this.TableName, fmt.Sprintf("本次写入：%d条", len(data)))
}
