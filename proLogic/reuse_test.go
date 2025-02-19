package reuse

import (
	"bytes"
	"filebase"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io/ioutil"
	"math"
	"stringbase"
	"testing"
	"time"
)

func TestGbToUtf8(t *testing.T) {
	/*GB编码格式转UTF_8*/
	testPath:="C:\\Users\\Administrator\\Desktop\\123.txt"
	strData:= filebase.ReadData(testPath)
	utf8byte,err:= GbToUtf8(stringbase.Str2bytes(strData))
	if err!=nil{
		t.Log(err)
		return
	}
	utf8data:=stringbase.Bytes2str(utf8byte)
	t.Log(utf8data)
}
func TestTimeToInt(t *testing.T) {
	resInt:=TimeToInt(time.Now())
	t.Log(resInt)
}
func TestEarthDisTance(t *testing.T) {
	resFloat:=EarthDistance(116.373762,40.10471,115.415378,37.942516)
	t.Log("距离为：",resFloat)
}
func GbToUtf8(s []byte) ([]byte, error) {
	//reader := transform.NewReader(byte.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	reader := transform.NewReader(bytes.NewReader(s),simplifiedchinese.GBK.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	return d, nil
}
/*时间转数字*/
func TimeToInt(nowTime time.Time) int {
	return nowTime.Year()*10000 + int(nowTime.Month())*100 + nowTime.Day()
}
/*坐标转距离 单位米*/
func EarthDistance(lat1, lng1, lat2, lng2 float64) float64 {
	radius := float64(6371000) // 6378137
	rad := math.Pi / 180.0

	lat1 = lat1 * rad
	lng1 = lng1 * rad
	lat2 = lat2 * rad
	lng2 = lng2 * rad

	theta := lng2 - lng1
	dist := math.Acos(math.Sin(lat1)*math.Sin(lat2) + math.Cos(lat1)*math.Cos(lat2)*math.Cos(theta))

	return dist * radius
}
