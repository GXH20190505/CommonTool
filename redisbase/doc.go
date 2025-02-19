package redisbase


/** redis 基础模块包说明
	1、使用方法
		使用ConnectRedisTest()函数确认redis是否能正常连接
		使用NewRedisMsgCache()函数创建一个redis操作对象
		使用RedisMsgCache对象的相关函数完成redis相关操作
	举例:
		revRedis:=newRedisCache("127.0.0.1",6379)
		revRedis.HGetAll("car")
*/


/** 修改记录
	2016-03-29 王成林
	1、添加列表操作
*/