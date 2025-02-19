//Redis基础层
//使用第三方的redis包进行redis通讯

//使用此包顺序说明
//1、通过newRedisCache()注册redis连接
//2、通过返回的RedisCache实现Redis的相关操作
//3、连接之前可以通过ConnectRedisTest（）测试能否正常连接Redis

//举例:
//revRedis:=newRedisCache("127.0.0.1",6379)
//revRedis.HGetAll("car")

package redisbase

import (
	//"fmt"

	"github.com/garyburd/redigo/redis"
)

type RedisMsgCache struct {
	pool    *redis.Pool
	Address string
}

func NewRedisMsgCache(redisAddr string, password string) *RedisMsgCache {
	pool, err := newPool(redisAddr, password, 0)
	if err != nil {
		return nil
	}
	return &RedisMsgCache{
		pool:    pool,
		Address: redisAddr,
		//Password: password,
	}
}

func NewRedisMsgCacheIndex(redisAddr string, password string, dbIndex int) *RedisMsgCache {
	pool, err := newPool(redisAddr, password, dbIndex)
	if err != nil {
		return nil
	}
	return &RedisMsgCache{
		pool:    pool,
		Address: redisAddr,
		//Password: password,
	}
}

/* 重写生成连接池方法
 */
func newPool(redisAddr string, password string, dbIndex int) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:   30,   //表示队列中空闲连接的数量
		MaxActive: 1000, //表示最大激活数量
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisAddr, redis.DialPassword(password), redis.DialDatabase(dbIndex))
			if err != nil {
				return nil, err
			}
			/*if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}*/
			return c, err
		},
	}, nil
}

/** 键值查询
输入为键值规则，查询全部键 dbkey=*
*/
func (self *RedisMsgCache) KEYS(dbkey string) ([]string, error) {
	return redis.Strings(self.Do("KEYS", dbkey))
}

/** 删除键值
 */
func (self *RedisMsgCache) DEL(dbkey string) error {
	_, err := self.Do("DEL", dbkey)
	return err
}

/** 字符串写入
 */
func (self *RedisMsgCache) Set(dbkey, dbvalue string) error {
	_, err := self.Do("SET", dbkey, dbvalue)
	return err
}

/** 字符串获取
 */
func (self *RedisMsgCache) Get(dbkey string) (string, error) {
	return redis.String(self.Do("GET", dbkey))
}

func (self *RedisMsgCache) HGetBool(dbkey string, dbfield string) (bool, error) {
	s, err := redis.Bool(self.Do("HGET", dbkey, dbfield))
	return s, err
}

/** 队列--左进
 */
func (self *RedisMsgCache) Lpush(dbkey string, dbvalue []byte) error {
	_, err := self.Do("LPUSH", dbkey, dbvalue)
	return err
}

/** 队列--右出
 */
func (self *RedisMsgCache) Rpop(dbkey string) ([]byte, error) {
	return redis.Bytes(self.Do("RPOP", dbkey))
}

/** 队列--队列长度
 */
func (self *RedisMsgCache) Llen(dbkey string) (int, error) {
	return redis.Int(self.Do("LLEN", dbkey))
}

/** 哈希--单条插入
 */
func (self *RedisMsgCache) HSet(dbkey string, dbfield string, dbvalue string) error {
	_, err := self.Do("HSET", dbkey, dbfield, dbvalue)
	//fmt.Println("HSET", dbkey, dbfield, dbvalue, err)
	return err
}

/** 哈希--多条插入
 */
func (self *RedisMsgCache) HMSet(dbkey string, mapfield map[string]string) error {
	_, err := self.Do("HMSET", redis.Args{}.Add(dbkey).AddFlat(mapfield)...)
	return err

}

/** 哈希--单条获取
 */
func (self *RedisMsgCache) HGet(dbkey string, dbfield string) (string, error) {
	s, err := redis.String(self.Do("HGET", dbkey, dbfield))
	//fmt.Println("HGET", dbkey, dbfield, s, err)
	return s, err
}

/** 哈希--全部获取
 */
func (self *RedisMsgCache) HGetAll(dbkey string) (map[string]string, error) {
	m, err := redis.StringMap(self.Do("HGETALL", dbkey))
	return m, err
}

/**
 hash HINCRBY
Redis Hincrby 命令用于为哈希表中的字段值加上指定增量值。

增量也可以为负数，相当于对指定字段进行减法操作。

如果哈希表的 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。

如果指定的字段不存在，那么在执行命令前，字段的值被初始化为 0 。
*/
func (self *RedisMsgCache) HINCRBY(dbkey string, dbfield string, incr int) error {
	_, err := self.Do("HINCRBY", dbkey, dbfield, incr)
	return err
}

/** 哈希--获取键值所有字段()
返回field
*/
func (self *RedisMsgCache) HKEYS(dbkey string) ([]string, error) {
	s, err := redis.Strings(self.Do("HKEYS", dbkey))
	return s, err
}

/** 哈希--删除单个字段
 */
func (self *RedisMsgCache) HDEL(dbkey string, dbfield string) (int, error) {
	i, err := redis.Int(self.Do("HDEL", dbkey, dbfield))
	return i, err

}

/** 集合--写入
 */
func (self *RedisMsgCache) SADD(dbkey string, dbvalue string) error {
	_, err := self.Do("SADD", dbkey, dbvalue)
	return err

}

/** 集合--多条写入
 */
func (self *RedisMsgCache) SADD_ALL(dbkey string, dbvalue []string) error {
	_, err := self.Do("SADD", dbkey, dbvalue)
	return err
}

/** 集合--读取
 */
func (self *RedisMsgCache) SMEMBERS(dbkey string) ([]string, error) {
	s, err := redis.Strings(self.Do("SMEMBERS", dbkey))
	return s, err
}

/** 集合--元素个数
 */
func (self *RedisMsgCache) SCARD(dbkey string) int {
	i, _ := redis.Int(self.Do("SCARD", dbkey))
	return i
}

/** 有序集合--写入
 */
func (self *RedisMsgCache) ZADD(dbkey string, dbscore string, dbmember string) error {
	_, err := self.Do("ZADD", dbkey, dbscore, dbmember)
	return err
}

/** 有序集合--删除元素
 */
func (self *RedisMsgCache) ZREM(dbkey string, dbmember string) error {
	_, err := self.Do("ZREM", dbkey, dbmember)
	return err
}

/** 有序集合--按照分数范围删除元素
删除分数在min和max之间的元素
如果不希望删除min和max，则在min和max之前添加"("
如果min="" 表示删除所有比max小的分数
如果max="" 表示删除所有比min大的分数
如果min和max 都="",则删除所有集合数据
*/
func (self *RedisMsgCache) ZREMRANGEBYSCORE(dbkey string, dbmin string, dbmax string) error {
	if dbmin == "" {
		dbmin = "-inf"
	}
	if dbmax == "" {
		dbmax = "+inf"
	}
	_, err := self.Do("ZREMRANGEBYSCORE", dbkey, dbmin, dbmax)
	return err

}

/** 有序集合--获得排名顺序 从小到大
dbkey 查询的键值
start 索引起始位，从0开始
stop  索引结束位，-1表示最后一个元素
	如 scores(1~5) value(v1~v5)
	则 start=0 stop =1 则返回v1 v2
showScores 是否显示分数，1表示显示 0表示不显示
*/
func (self *RedisMsgCache) ZRANGE(dbkey string, start int, stop int, showScores int) (map[string]string, error) {
	//if showScores > 0 {
	return redis.StringMap(self.Do("ZRANGE", dbkey, start, stop, "WITHSCORES"))
	//} else {
	//	return redis.StringMap(c.Do("ZRANGE", dbkey, start, stop))
	//}
	return nil, nil
}

/** 有序集合--获得排名顺序 从大到小
dbkey 查询的键值
start 索引起始位，从0开始
stop  索引结束位，-1表示最后一个元素
	如 scores(1~5) value(v1~v5)
	则 start=0 stop =1 则返回v5 v4
showScores 是否显示分数，1表示显示 0表示不显示
返回值：map[string]string key=member  value=scorse
*/
func (self *RedisMsgCache) ZREVRANGE(dbkey string, start int, stop int, showScores int) (map[string]string, error) {
	//if showScores > 0 {
	return redis.StringMap(self.Do("ZREVRANGE", dbkey, start, stop, "WITHSCORES")) //返回map
	//} else {
	//	return redis.StringMap(c.Do("ZREVRANGE", dbkey, start, stop))	//返回数组
	//}
	return nil, nil
}

/** 有序集合--获取指定分数范围的元素
区别，上面两个命令是按索引查询，本命令按照分数进行查询
*/
func (self *RedisMsgCache) ZRANGEBYSCORE(dbkey string, dbmin string, dbmax string) (map[string]string, error) {
	return redis.StringMap(self.Do("ZRANGEBYSCORE", dbkey, dbmin, dbmax, "WITHSCORES"))
}

/** 查看redis有序集合苏韩剧长度
 */
func (self *RedisMsgCache) ZCARD(dbkey string) (int, error) {
	return redis.Int(self.Do("ZCARD", dbkey))
}

/** 添加超时时间 2016-06-03 WCL
time 超时间隔，单位：秒
*/
func (self *RedisMsgCache) EXPIRE(key string, second int) error {
	_, err := self.Do("EXPIRE", key, second)
	return err
}

/** 添加超时时间
time 超时间隔，单位：毫秒
*/
func (self *RedisMsgCache) PEXPIRE(key string, millsecond int) error {
	_, err := self.Do("PEXPIRE", key, millsecond)
	return err
}

/** 添加超时时间
time 时间戳
*/
func (self *RedisMsgCache) EXPIREAT(key string, unix int) error {
	_, err := self.Do("EXPIREAT", key, unix)
	return err
}

/** 查看超时剩余时间
 */
func (self *RedisMsgCache) TTL(key string) int {
	i, _ := redis.Int(self.Do("TTL", key))
	return i
}

/**
 */
func (self *RedisMsgCache) PTTL(key string) int {
	i, _ := redis.Int(self.Do("TTL", key))
	return i
}

func (self *RedisMsgCache) LMore(dbkey string, start, end int) ([]string, error) {
	return redis.Strings(self.Do("LRANGE", dbkey, start, end))
}

/** 自定义操作 2016-06-06 WCL
自己执行Redis指令，方便测试
*/
func (self *RedisMsgCache) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	c := self.pool.Get()
	defer c.Close()
	//t1 := time.Now()
	reply, err = c.Do(commandName, args...)
	//s := time.Now().Sub(t1).Seconds()
	//if s >= 0.001 {
	//	errorlog.ErrorLogDebug("redis", commandName, fmt.Sprintf("t:%f", s))
	//}
	return
}

func (self *RedisMsgCache) INCR(key string) error {
	_, err := self.Do("INCR", key)
	return err
}

func (self *RedisMsgCache) String(res interface{}, err error) (string, error) {
	return redis.String(res, err)
}

/** 互斥锁
如果可以写入，则返回1，否则返回0
通过控制此锁，可以通过竞争机制解决高并发获取权限
*/
func (self *RedisMsgCache) SETNX(key string, value string) (int, error) {
	return redis.Int(self.Do("SETNX", key, value))

}

func (self *RedisMsgCache) HExists(dbkey string, dbfield string) (bool, error) {
	s, err := redis.Bool(self.Do("hexists", dbkey, dbfield))
	return s, err
}

func (self *RedisMsgCache) Ping() (string, error) {
	return redis.String(self.Do("PING"))
} 