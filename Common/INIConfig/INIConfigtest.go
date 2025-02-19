package INIConfig

import (
	"fmt"
)
//[MODBUS]      session
//ID = 1,2,3    key为ID
func IniBaseTest() {
	strPath:="C:\\Users\\Administrator\\Desktop\\安装包\\config\\factor.ini"
   data,error:= GetIniValue(strPath,"MODBUS","ID")
   if error!=nil{
   	fmt.Println(error)
	   return
   }
   fmt.Println(data)//输出1，2，3
	result:=SetIniValue(strPath,"MODBUS","ID","1,2,3,4,5,6")
   if result{
   	fmt.Println("写入成功")  //ID=1,2,3,4,5,6
	   return
   }
   fmt.Println("写入失败")
}
