package P2PClient

import (
	"time"
	"timebase"
)

var (
	//服务编码
	ServiceCodeInfo string
	//休眠时间
	sleep time.Duration = time.Duration(1) * time.Second
	//客户端连接对象
	ServiceClient *Client
	//配置文件路径
	ConfigAddress string = "config/settings.ini"
	//心跳时间
	ServiceLastTime string = timebase.NowTimeFormat()
	//离线时间(秒)
	Uploadinterval float64 = 90
	//发送心跳标识
	upLoadFLag bool = true

	//注册成功标识
	registerFlag bool = false
	//注册信息
	RegisterBaseInfo *ConfigService
	//配置信息
	ConfigBaseInfo *ConfigService
	//心跳信息
	HeartBeatInfo *ServiceInfo
	//子服务信息
	SubServices = make(map[string]*ServerInfo)
	//断包报文
	AppendStr string
	//数据库配置个数 默认为1
	DbCount int = 1
	//系统类型linux windows
	SysType = "linux"
)

type LinuxSys string
type WindowsSys string

//子服务运行路径和名称
type ServerInfo struct {
	//Path  string //路径
	Name  string //名称
	State int    //子服务的状态 1:启动状态 0:未启动状态  状态由平台下发
}
