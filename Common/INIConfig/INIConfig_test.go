package INIConfig

import (
	"testing"
)


func TestGetIniValue(t *testing.T) {
	filepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\factor.ini"
	data,error:= GetIniValue(filepath,"MODBUS","ID")
	if error!=nil{
		t.Log(error)
		return
	}
	t.Log(data)//输出1，2，3
}

func TestSetIniValue(t *testing.T) {
	filepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\factor.ini"
	result:=SetIniValue(filepath,"MODBUS","ID","1,2,3,4,5,6")
	if result{
	t.Log("写入成功")  //ID=1,2,3,4,5,6
		return
	}
	t.Log("写入失败")
}
