package INIConfig

import (
	"errors"
	"log"

	"github.com/goconfig"
)

// <summary>
// ini读取操作
// </summary>
// <param name="filePath">配置文件路径</param>
// <param name="session">标识</param>
// <param name="key">键值</param>
// <returns>值</returns>
func GetIniValue(PathStr, session, key string) (string, error) {
	iniConfig, err := goconfig.LoadConfigFile(PathStr)
	if err != nil {
		log.Println("读取配置文件失败,%s", err)
		return "", errors.New("读取配置文件失败")
	} else {
		info, _ := iniConfig.GetValue(session, key)
		return info, nil
	}
}

// <summary>
// ini写入操作
// </summary>
// <param name="filePath">配置文件路径</param>
// <param name="session">标识</param>
// <param name="key">键值</param>
// <param name="value">键对应的值</param>
// <returns>执行结果</returns>
func SetIniValue(PathStr, session, key, value string) bool {
	var flag bool
	iniConfig, err := goconfig.LoadConfigFile(PathStr)
	if err != nil {
		log.Println("读取配置文件失败,%s", err)
		return false
	} else {
		flag = iniConfig.SetValue(session, key, value)
		err = goconfig.SaveConfigFile(iniConfig, PathStr)
		if err == nil {
			flag = true
		}
	}
	return flag
}
