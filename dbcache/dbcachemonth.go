package dbcache

import (
	"errorlog"
	"mysqlbase"
	"sync"
	"time"
)

/** 按月分表，表字段一致
 */
type DBCacheMonth struct {
	MysqlDB  *mysqlbase.MysqlCache
	TabCache map[string]*DBCache //key=table name
	TabTime  map[string]time.Time
	dataLock sync.RWMutex
	Columns  interface{}
}

func NewCacheMonth(mysqlDB *mysqlbase.MysqlCache, cols interface{}) *DBCacheMonth {
	db := new(DBCacheMonth)
	//db.TableName = tabname
	db.MysqlDB = mysqlDB
	db.Columns = cols
	db.TabCache = make(map[string]*DBCache)
	db.TabTime = make(map[string]time.Time)
	go db.Start()
	return db
}

func (this *DBCacheMonth) Start() {
	for {
		this.clear_tab()
		time.Sleep(1 * time.Minute)
	}
}

func (this *DBCacheMonth) AppendData(tabName string, js interface{}) {
	this.dataLock.Lock()
	defer this.dataLock.Unlock()

	cs, ok := this.TabCache[tabName]
	if ok == false {
		cs = NewCache(this.MysqlDB, tabName, this.Columns)
		this.TabCache[tabName] = cs
		errorlog.ErrorLogDebug("dbmonth", tabName, cs.colValue)
	}
	cs.AppendData(js)
	this.TabTime[tabName] = time.Now()
}

func (this *DBCacheMonth) clear_tab() {
	this.dataLock.Lock()
	defer this.dataLock.Unlock()
	for k, t := range this.TabTime {
		if time.Since(t).Minutes() > 60.0 { //1小时没有收到新报文，剔除该表
			delete(this.TabCache, k)
		}
	}

}
