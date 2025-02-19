package P2PClient

import (
	"errors"
	"inibase"
	"strconv"
)

var MyIniFile *inibase.IniFile

// <summary>
// 初始化config
// </summary>
func IniConfig() error {
	if ConfigAddress != "" {
		MyIniFile = inibase.NewIniConfig(ConfigAddress)
		return nil
	}
	return errors.New("初始化配置文件失败")
}

// <summary>
// 初始化配置信息
// </summary>
func NewConfigServer() *ConfigService {

	var db = make([]*DbT, 2)
	for i := 0; i < 2; i++ {
		db[i] = new(DbT)
		db[i].Data_type 	= "1"
		db[i].Db_instance 	= ""
		db[i].Db_ip 		= ""
		db[i].Db_password 	= ""
		db[i].Db_port 		= ""
		db[i].Db_type 		= ""
		db[i].Db_user 		= ""
		db[i].Remark 		= ""
	}
	return &ConfigService{
		Type: "",
		Message: Message{
			Service_name		: 	"",
			Service_code 		:  	"",
			Service_ip 			:  	"",
			Service_port 		:  	"",
			Soft_version 		:  	"",
			Watchdog_code		: 	"",
			Service_addr		:   "",
			Servicetype_code	:  	"",
			Protocaltype_code	: 	"",
			Db					:	db},
	}
}

// <summary>
// 初始化参数读取
// </summary>
// <param name="path">配置文件路径(例:读取根目录下路径(config/setting.ini))</param>
// <returns></returns>
func IniInfo() (ServerIp string, ServerPort string, err error) {

	//读取配置文件
	configInfo, IP, Port := GetIniConfig()

	//初始化注册信息和配置信息
	ConfigBaseInfo = configInfo
	ConfigBaseInfo.Type = "config"

	RegisterBaseInfo = configInfo
	RegisterBaseInfo.Type = "register"

	if IP == "" || Port == "" {
		return "", "", errors.New("read data error")
	}

	ServerIp = IP
	ServerPort = Port
	return ServerIp, ServerPort, nil
}

// <summary>
// 自动保存配置文件
// </summary>
// <returns></returns>
func (config *ConfigService) AutoSaveConfig() error {

	IniConfig()

	MyIniFile.WriteString("PLATFORM", "PLATFORMIP", "")
	MyIniFile.WriteString("PLATFORM", "PLATFORMPORT", "")
	MyIniFile.WriteString("SYSTEM", "DBCOUNT", strconv.Itoa(DbCount))
	if err := SetIniConfig(config); err != nil {
		return err
	}
	return nil
}

// <summary>
// 读取ini配置数据
// </summary>
// <param name="filePath">配置文件路径</param>
// <returns>名称对应的值</returns>
func GetIniConfig() (*ConfigService, string, string) {

	var configInfo = &ConfigService{Type: "", Message: Message{Db: make([]*DbT, 0)}}

	ip 				:= MyIniFile.ReadString("PLATFORM", "PLATFORMIP", 	"")
	port 			:= MyIniFile.ReadString("PLATFORM", "PLATFORMPORT", "")
	serviceDogCode 	:= MyIniFile.ReadString("PLATFORM", "WATCHDOGCODE", "")

	serviceIP 		:= MyIniFile.ReadString("NETWORK", "IP", 	"")
	servicePort 	:= MyIniFile.ReadString("NETWORK", "PORT", 	"")

	serviceName 	:= MyIniFile.ReadString("SYSTEM", "SERVICENAME", 	"")
	serviceCode 	:= MyIniFile.ReadString("SYSTEM", "SERVICECODE", 	"")
	serviceVersion 	:= MyIniFile.ReadString("SYSTEM", "SOFTVERSION", 	"")
	serviceAddr 	:= MyIniFile.ReadString("SYSTEM", "SERVICEADDR", 	"")
	serviceTypeCode := MyIniFile.ReadString("SYSTEM", "SERVICETYPECODE", "")
	protocalTypeCode:= MyIniFile.ReadString("SYSTEM", "SERVICEPROTRCOL", "")
	dbCount 		:= MyIniFile.ReadString("SYSTEM", "DBCOUNT", 		"0")
	DbCount, _ 		= strconv.Atoi(dbCount)

	//数据库参数配置
	for i := 0; i < DbCount; i++ {
		dbType 		:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DBTYPE", 		"")
		dbIP 		:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DBIP", 		"")
		dbPort 		:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DBPORT", 		"")
		dbInstance 	:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DBINSTANCE", 	"")
		dbUser 		:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DBUSER", 		"")
		dbPwd 		:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DBPWD", 		"")
		dataType 	:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "DATATYPE", 	"")
		dbRemark 	:= MyIniFile.ReadString("DB"+strconv.Itoa(i+1), "REMARK", 		"")

		db := &DbT{Db_type: dbType, Db_ip: dbIP, Db_port: dbPort, Db_instance: dbInstance,
			Db_user: dbUser, Db_password: dbPwd, Data_type: dataType, Remark: dbRemark}

		configInfo.Db = append(configInfo.Db, db)

	}

	//服务名称配置
	configInfo.Service_name 	= serviceName
	configInfo.Service_code 	= serviceCode
	configInfo.Service_ip 		= serviceIP
	configInfo.Service_port 	= servicePort
	configInfo.Soft_version 	= serviceVersion
	configInfo.Watchdog_code 	= serviceDogCode
	configInfo.Service_addr 	= serviceAddr
	configInfo.Servicetype_code = serviceTypeCode
	configInfo.Protocaltype_code = protocalTypeCode
	return configInfo, ip, port
}

// <summary>
// 更新ini配置数据
// </summary>
// <param name="filePath">配置文件路径</param>
// <returns>名称对应的值</returns>
func SetIniConfig(info *ConfigService) error {
	var flag bool = true

	serviceName 	:= info.Service_name
	serviceCode 	:= info.Service_code
	serviceIP 		:= info.Service_ip
	servicePort 	:= info.Service_port
	serviceVersion 	:= info.Soft_version
	serviceDogCode 	:= info.Watchdog_code
	serviceAddr 	:= info.Service_addr
	serviceTypeCode := info.Servicetype_code
	protocalTypeCode := info.Protocaltype_code

	if ok := MyIniFile.WriteString("SYSTEM", "SERVICENAME", serviceName); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("SYSTEM", "SERVICECODE", serviceCode); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("NETWORK", "IP", serviceIP); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("NETWORK", "PORT", servicePort); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("SYSTEM", "SOFTVERSION", serviceVersion); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("PLATFORM", "WATCHDOGCODE", serviceDogCode); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("SYSTEM", "SERVICEADDR", serviceAddr); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("SYSTEM", "SERVICETYPECODE", serviceTypeCode); ok == false {
		flag = false
		goto exsit
	}
	if ok := MyIniFile.WriteString("SYSTEM", "SERVICEPROTRCOL", protocalTypeCode); ok == false {
		flag = false
		goto exsit
	}
	//数据库参数配置

	for i, data := range info.Db {
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DBTYPE", data.Db_type); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DBIP", data.Db_ip); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DBPORT", data.Db_port); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DBINSTANCE", data.Db_instance); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DBUSER", data.Db_user); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DBPWD", data.Db_password); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "DATATYPE", data.Data_type); ok == false {
			flag = false
			goto exsit
		}
		if ok := MyIniFile.WriteString("DB"+strconv.Itoa(i+1), "REMARK", data.Remark); ok == false {
			flag = false
			goto exsit
		}
	}

exsit:
	if flag == true {
		return nil
	} else {
		return errors.New("更新出错")
	}
}
