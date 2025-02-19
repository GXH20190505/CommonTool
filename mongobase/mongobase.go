package mongobase

import (
	"context"
	"fmt"
	"github.com/qiniu/qmgo"
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
type MongoCache struct {
	ConnDB     *qmgo.QmgoClient
	ConStr     string
	DBAddr     string
	DBInstance string
	DBUser     string
	DBPwd      string
}

/** 新建达梦数据库缓存
"dm://SYSDBA:wanwei123@192.168.1.160:5236?BOOKSHOP&charset=utf8"
*/
func NewMongoConnection(dbIp,dbPort string, dbInstance string, dbUser string, dbPwd string) (*MongoCache, error) {
	var err error
	rs := &MongoCache{
		ConnDB:     nil,
		ConStr:     fmt.Sprintf("mongodb://%s:%s",dbIp,dbPort),
		DBUser:     dbUser,
		DBPwd:      dbPwd,
		DBAddr:     "",
		DBInstance: dbInstance,
	}
	rs.ConnDB, err = qmgo.NewMongoConnection(dbIp, dbPort, dbInstance, dbUser, dbPwd)
	if err != nil {
		return nil, err
	}
	return rs, nil

}

func (self *MongoCache) GetCollectList(ctx context.Context)([]string,error)  {
	// 获取所有集合名称
	collections,err:=self.ConnDB.Database.GetCollectionNames(ctx)
	if err!=nil{
		return nil,err
	}
	return collections,nil
}

// InsertOne 插入一条记录到MongoDB集合中。
func (self *MongoCache) InsertOne(ctx context.Context,collectionName string, selector map[string]interface{})(*qmgo.InsertOneResult,error) {
	collect:=self.ConnDB.Database.Collection(collectionName)
	r,err:= collect.InsertOne(ctx,selector)
	return r,err
}

// InsertMany 向指定集合插入多条文档。

func (self *MongoCache) InsertMany(ctx context.Context, collectionName string, selector []map[string]interface{})(*qmgo.InsertManyResult,error) {
	collect:=self.ConnDB.Database.Collection(collectionName)
	r,err:= collect.InsertMany(ctx,selector)
	return r,err
}
// BulkInsertOne 批量插入单个文档到MongoDB集合中。
func (self *MongoCache) BulkInsertOne(ctx context.Context, collectionName string, selector map[string]interface{})(*qmgo.BulkResult,error) {
	collect:=self.ConnDB.Database.Collection(collectionName)
	b,err:= collect.Bulk().InsertOne(selector).Run(ctx)
	if err!=nil{
		return nil,err
	}
	return b,err
}
// BulkInsertMany 批量插入文档到MongoDB集合中。
func (self *MongoCache) BulkInsertMany(ctx context.Context,collectionName string, selector []map[string]interface{})(*qmgo.BulkResult,error) {
	collect:=self.ConnDB.Database.Collection(collectionName)
	bulk:=collect.Bulk()
	for _, i := range selector {
		bulk.InsertOne(i)
	}
	b,err:= bulk.Run(ctx)
	if err!=nil{
		return nil,err
	}
	return b,err
}
// UpDataOne 更新MongoDB中的单个文档
func (self *MongoCache) UpDataOne(ctx context.Context,collectionName string, selector map[string]interface{},updata map[string]interface{})(error) {
	collect:=self.ConnDB.Database.Collection(collectionName)
	err:= collect.UpdateOne(ctx,selector,updata)
	return err
}
// UpSert 在MongoDB集合中执行插入或更新操作
func (self *MongoCache) UpSert(ctx context.Context,collectionName string, selector map[string]interface{},updata map[string]interface{})(*qmgo.UpdateResult,error) {
	collect:=self.ConnDB.Database.Collection(collectionName)
	r, err:= collect.Upsert(ctx,selector,updata)
	return r, err
}

/*BulkUpsertMany 在MongoDB集合中执行批量插入或更新操作
selectors := []map[string]interface{}{
	{
		"filter": bson.M{"name": "Alice"},
		"update": bson.M{"$set": bson.M{"age": 30}},
	},
	{
		"filter": bson.M{"name": "Bob"},
		"update": bson.M{"$set": bson.M{"age": 25}},
	},
}*/
type UpsertModel struct {
	Filter  map[string]interface{}
	Update  map[string]interface{}
}

func (self *MongoCache) BulkUpsertMany(ctx context.Context, collectionName string, selectors []UpsertModel) (*qmgo.BulkResult, error) {
	collect := self.ConnDB.Database.Collection(collectionName)
	bulk := collect.Bulk().SetOrdered(false)

	// 构建批量更新或插入模型
	for _, selector := range selectors {
		bulk.Upsert(selector.Filter, selector.Update)
	}

	// 执行批量操作
	result, err := bulk.Run(ctx)
	if err!= nil {
		return nil, err
	}

	return result, nil
}
//FindDataOne 查询单台数据
func (self *MongoCache) FindDataOne(ctx context.Context, collectionName string, selector map[string]interface{},sort string)(map[string]interface{}, error) {
	collection := self.ConnDB.Database.Collection(collectionName)
	var data map[string]interface{}
	err := collection.Find(ctx, selector).Sort(sort).One(&data)
	if err != nil {
		return nil,err
	}
	return data, nil
}
//FindDataLimit 查询指定条数数据
func (self *MongoCache) FindDataLimit(ctx context.Context, collectionName string, selector map[string]interface{},sort string,limit int64)([]map[string]interface{}, error) {
	collection := self.ConnDB.Database.Collection(collectionName)
	var data []map[string]interface{}
	err := collection.Find(ctx, selector).Sort(sort).Limit(limit).All(&data)
	if err != nil {
		return nil,err
	}
	return data, nil
}
//FindDataAll 查询所有符合条件数据
func (self *MongoCache) FindDataAll(ctx context.Context, collectionName string, selector map[string]interface{},sort string)([]map[string]interface{}, error) {
	collection := self.ConnDB.Database.Collection(collectionName)
	var data []map[string]interface{}
	err := collection.Find(ctx, selector).Sort(sort).All(&data)
	if err != nil {
		return nil,err
	}
	return data, nil
}


//由于版本不同，导致qmgo和mgo的索引创建方式不同，暂时未找到合适的方法，先注释掉
//CreateIndex 创建普通索引和联合索引
/*func (self *MongoCache) CreateIndex(ctx context.Context, collectionName string, index []string) error {
	collection := self.ConnDB.Database.Collection(collectionName)
	// 创建普通索引
	var normalIndex = options.IndexModel{
		Key: index,
	}
	err := collection.CreateIndexes(ctx,[]options.IndexModel{normalIndex})
	if err != nil {
		return err
	}
	return  nil
}
//CreateIndex 创建唯一索引
func (self *MongoCache) CreateUnionIndex(ctx context.Context, collectionName string, index []string) error {
	collection := self.ConnDB.Database.Collection(collectionName)
	bol:=true
	uniqueIndex:=options.IndexModel{}
	uniqueIndex.Key=index
	uniqueIndex.Unique=&bol
	
	err := collection.CreateIndexes(ctx, []options.IndexModel{uniqueIndex})
	if err != nil {
		return err
	}
	return  nil
}*/
