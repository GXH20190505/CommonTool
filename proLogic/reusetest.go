package reuse

import (
	"bytes"
	"filebase"
	"fmt"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io/ioutil"
	"math"
	"stringbase"
	"time"
)
func reuseTest() {
	/*GB编码格式转UTF_8*/
  testPath:="C:\\Users\\Administrator\\Desktop\\123.txt"
  strData:= filebase.ReadData(testPath)
  utf8byte,err:= GbToUtf8(stringbase.Str2bytes(strData))
  if err!=nil{
  	fmt.Println(err)
	  return
  }
  utf8data:=stringbase.Bytes2str(utf8byte)
  fmt.Println(utf8data)

  /*时间转数字，返回20210926*/
  resInt:=TimeToInt(time.Now())
  fmt.Println(resInt)
  /*坐标转距离 单位米*/
	resFloat:=EarthDistance(116.373762,40.10471,115.415378,37.942516)
	fmt.Println("距离为：",resFloat)

}




/*GB转utf8*/
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

