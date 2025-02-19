package inibase
import (
	"fmt"
	"testing"
)
 var resInstance *IniFile
func GetNewIniConfig()*IniFile {
	filepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\factor.ini"
	if resInstance!=nil{
		return resInstance
	}
	/*通过文件路径新建文件*/
	resInstance:=NewIniConfig(filepath)
	if resInstance==nil{
		fmt.Println("文件创建失败")
	}
	fmt.Printf("文件创建成功")
	return resInstance
}
func TestNewIniConfig(t *testing.T) {
	filepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\factor.ini"
	/*通过文件路径新建文件*/
	resultCreate:=NewIniConfig(filepath)
	if resultCreate==nil{
		t.Log("文件创建失败")
	}
	t.Log("文件创建成功")
}
func TestIniFileExsit(t *testing.T) {
	filepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\factor.ini"
	resultExit:=IniFileExsit(filepath)
	if !resultExit{
		t.Log("文件不存在",resultExit)
		return
	}
	t.Log("文件存在")
}
func TestIniFile_ReadString(t *testing.T) {
	_resInstance:=GetNewIniConfig()
	/*读取配置信息,
	[MODBUS]          section
	ID = 1,2,3,4,5,6  key为ID
	*/
	resultRead:=_resInstance.ReadString("MODBUS","ID","1")
	t.Log("读取数据为",resultRead)
}
func TestIniFile_WriteString(t *testing.T) {
	/*写入配置信息*/
	_resInstance:=GetNewIniConfig()
	resultWrite:=_resInstance.WriteString("MODBUS","ID","1,2,3,4,5,6,7,8,9")
	if !resultWrite {
		t.Log("写入失败")
		return
	}
	t.Log("写入成功")
}
func TestIniFile_GetSection(t *testing.T) {
	_resInstance:=GetNewIniConfig()
	resultSection,err :=_resInstance.GetSection("MODBUS_ID_1")
	if err!=nil{
		fmt.Println("读取所有信息失败")
		return
	}
	fmt.Println("读取所有信息成功")
	for s, s2 := range resultSection {
		fmt.Println(s,s2)
	}
}
