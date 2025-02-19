/** redisbase相关函数测试
 */

package main

import (
	"fmt"
	"time"

	"redisbase"
)

var RedisClient *redisbase.RedisMsgCache

func main() {
	//Redis连接

	if err := redisbase.ConnectRedisTest("127.0.0.1:6379", ""); err != nil {
		fmt.Println(err)
		return
	}

	RedisClient = redisbase.NewRedisMsgCache("127.0.0.1:6379", "")
	TestHSet()
}

func testExpire() {
	//写入key
	key := "test:expire"

	RedisClient.Set(key, "20")
	RedisClient.EXPIRE(key, 20)

	for {
		s := RedisClient.TTL(key)
		fmt.Println("expire time:", key, s)
		if s <= 0 {
			break
		}
		time.Sleep(time.Second)
	}

}

func TestHSet() {
	key := "test:strings"
	for i := 0; i < 50; i++ {
		RedisClient.SADD(key, fmt.Sprintf("I%d", i))
	}
	RedisClient.EXPIRE(key, 30)
	i := RedisClient.SCARD(key)
	fmt.Println(i)
}
