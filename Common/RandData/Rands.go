package RandData

import (
	"math/rand"
	"time"
)

// <summary>
//读取随机数
// </summary>
func Rand(min, max int) int {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	maxBigInt := r.Intn(max)
	if maxBigInt < min {
		Rand(min, max)
	}
	return maxBigInt
}
