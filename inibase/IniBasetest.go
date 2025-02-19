package inibase

import (
	"fmt"
)

func IniBaseTest() {
	filePath := "C:\\Users\\Administrator\\Desktop\\安装包\\config\\factor.ini"

	/*通过获取文件路径检查文件夹是否存在*/
	resultExit:= IniFileExsit(filePath)
	if !resultExit{
		fmt.Println("文件不存在",resultExit)
		return
	}
	fmt.Println("文件存在")


	/*通过文件路径新建文件*/
	resultCreate:= NewIniConfig(filePath)
	if resultCreate==nil{
		fmt.Println("文件创建失败")
		return
	}
	fmt.Println("文件创建成功")



	/*读取配置信息,
	[MODBUS]          section
	ID = 1,2,3,4,5,6  key为ID
	*/
	resultRead:=resultCreate.ReadString("MODBUS","ID","1")
	fmt.Println("读取数据为",resultRead)
	/*写入配置信息*/
	resultWrite:=resultCreate.WriteString("MODBUS","ID","1,2,3,4,5,6,7,8,9")
	if !resultWrite {
		fmt.Println("写入失败")
		return
	}
	fmt.Println("写入成功")


	/*读取标签所有信息  内容count = 3，返回 count 3*/
	resultSection,err :=resultCreate.GetSection("MODBUS_ID_1")
	if err!=nil{
		fmt.Println("读取所有信息失败")
		return
	}
	fmt.Println("读取所有信息成功")
	for s, s2 := range resultSection {
		fmt.Println(s,s2)
	}
}
