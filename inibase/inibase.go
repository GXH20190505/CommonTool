//ini文件配置操作
//使用方法
//1、通过SpecifyIni()配置ini文件地址
//2、如果不先配置ini文件地址，则无法正常读取或配置 ini文件
//举例
//if err := inibase.SpecifyIni("config/settings.ini");err!=nil{
//	return err
//}
//r_rev_IP := inibase.ReadString("DB_Redis_Rev","IP","")

package inibase

import (
	"filebase"
	"fmt"
	_ "fmt"

	"github.com/Unknwon/goconfig"
)

type IniFile struct {
	Address   string
	MyIniFile *goconfig.ConfigFile
}

func IniFileExsit(IniAddr string) bool {
	if ok := filebase.CheckFileIsExist(IniAddr); ok == false {
		filebase.GetFilePath(IniAddr) //通过获取文件路径检查文件夹是否存在
		filebase.WriteData(IniAddr, "")
		return false
	}
	return true
}

func NewIniConfig(IniAddr string) *IniFile {
	//没有需要自动创建
	tempIni, err := goconfig.LoadConfigFile(IniAddr)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return &IniFile{
		Address:   IniAddr,
		MyIniFile: tempIni,
	}
}

/*读取配置信息

 */
func (ini *IniFile) ReadString(section string, key string, defaultStr string) (value string) {
	value, err := ini.MyIniFile.GetValue(section, key)
	if err != nil || value == "" {
		ini.WriteString(section, key, defaultStr)
		return defaultStr
	}
	//fmt.Println(section,key,value)
	return value
}

/* 写配置信息
 */
func (ini *IniFile) WriteString(section string, key string, value string) bool {
	flag := ini.MyIniFile.SetValue(section, key, value)
	err := goconfig.SaveConfigFile(ini.MyIniFile, ini.Address)
	if err == nil {
		flag = true

	}
	return flag
}

/** 读取标签所有信息
 */
func (ini *IniFile) GetSection(section string) (map[string]string, error) {
	return ini.MyIniFile.GetSection(section)
}
