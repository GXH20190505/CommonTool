package P2PInit

import (
	"P2PClient"
	"errors"
	"flag"
	"runtime"
)

var (
	platformIP   string //平台服务IP
	platformPort string //平台服务端口
	err          error  //
)

func P2PInit() (string, int32, error) {

	P2PClient.ConfigAddress = "config/settings.ini"

	sys := runtime.GOOS
	switch sys {
	case "linux":
		P2PClient.SysType = "linux"
	case "windows":
		P2PClient.SysType = "windows"
	}

	//初始化配置文件
	P2PClient.IniConfig()

	//初始化参数信息
	platformIP, platformPort, err = P2PClient.IniInfo()
	if err == nil {
		P2PClient.ServiceCodeInfo = P2PClient.ConfigBaseInfo.Service_code

		//开始连接服务端
		addr := flag.String("addr", platformIP+":"+platformPort, "message server address and port ,default 127.0.0.1:8443")
		user := flag.Int("uid", 10000, "user login id,default 10000")
		flag.Parse()

		return *addr, int32(*user) + int32(1), nil
	} else {
		return "", 3000, errors.New("读取配置文件失败")
	}
	return "", 3000, errors.New("读取配置文件失败")
}
