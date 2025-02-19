package stringbase

import (

	"testing"
)

func TestBytes2str(t *testing.T) {
    resByt:=Str2bytes("asdfg")
    resStr:=Bytes2str(resByt)
    t.Log(resStr)
}

func TestFloat64(t *testing.T) {
	resFloat64:=Float64("123.1")
	t.Log("",resFloat64)
}
/** 数据小数点位数处理
保留制定位数的小数位
如果Data="" 或者 Point="" 退出处理
处理逻辑，在Data的小数位后面添加Point个数个0，然后进行截取
point为要保留的位数，point=3时，123.45返回123.450

*/
func TestFormatDataPoint(t *testing.T) {
	resStr:=FormatDataPoint("123.45","2")
	t.Log("保留小数",resStr)
}

func TestGetAvgValue(t *testing.T) {
	resStr:=GetAvgValue("2,4,6,8")
	t.Log("平均值",resStr)
}

func TestGetCouValue(t *testing.T) {
	resStr:=GetCouValue("2,4,6,8")
	t.Log("累计值",resStr)
}

func TestGetInsertStr(t *testing.T) {
	resStr:=GetInsertStr("abcdefg","b","e")
	t.Log("获取字符串",resStr)
	resStr=""
}

func TestGetMaxString(t *testing.T) {
	resStr:=GetMaxString("10,8,5,7,13,17")
	t.Log("最大值",resStr)
}

func TestGetMaxValue(t *testing.T) {
	resStr:=GetMaxValue("2,4,4,6,8")
	t.Log("最大值",resStr)
}

func TestGetMinValue(t *testing.T) {
	resStr:=GetMinValue("10,8,5,7,13,17")
	t.Log("最小值",resStr)
}

func TestInt(t *testing.T) {
	resInt:=Int("123.1")
	t.Log("数据转float64",resInt)
}

func TestIsRealString(t *testing.T) {
    resBool:=IsRealString("a1b2d3")
    t.Log(resBool)
}

func TestJsonToString(t *testing.T) {
	type  mess struct {
		Name string
		Age  int
	}
	messData:=new(mess)
	messData.Name="MIKE"
	messData.Age=18
	resStr:=JsonToString(messData)
	t.Log("json转string",resStr)
	resStr=""
}

func TestStr2bytes(t *testing.T) {
     resStr:=Str2bytes("123456")
     t.Log(resStr)
}

func TestSubstrByByte(t *testing.T) {
	resStr:=SubstrByByte("abcde",3)
	t.Log("字符串截取",resStr)
}

func TestUnitOfbits(t *testing.T) {
	resStr:=UnitOfbits(10)
	t.Log("流量单位转换",resStr)
}
