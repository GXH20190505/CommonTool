package RandData

import (
	"testing"
	"time"
)

func TestRand(t *testing.T) {
	for i:=0;i<10;i++ {
		randData:=Rand(10,100)//根据时间种子生成随机数
		t.Log(randData)
		time.Sleep(1*time.Second)//不加会出现重复随机数
	}
}
