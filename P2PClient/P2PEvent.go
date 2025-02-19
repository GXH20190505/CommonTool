package P2PClient

import (
	"bytes"
	"errorlog"
	"fmt"
	_"log"
	"strconv"
	"strings"
	"time"
	"timebase"
	"github.com/jason"
)

// <summary>
// 客户端连接服务端
// </summary>
// <param name="wg">线程</param>
// <param name="addr">IP地址和端口</param>
// <param name="uid">用户id</param>
// <param name="key">标识</param>
// <returns></returns>
func Doconnect(addr string, uid int32, key string) {
	//判断数据是否启用接收 true:启用接收接收 false:没有启动数据接收
	var IsRev bool

	ServiceClient = NewClient(addr, uid, key)
	
	//循环判断连接是否断开
	for {
		if ServiceClient.IsLogin() {
			if IsRev == true {
				IsRev = false
				//开启注册线程
				go SendRegisterInfo()
			}
		} else {
			ServiceClient.Logout()
			ServiceClient.Handshake()
			IsRev = true	//每次重连后注册一次服务
		}
		time.Sleep(sleep)
	}

}

// <summary>
// 线程定时接收数据
// </summary>
// <param name=""></param>
// <returns></returns>
func GetRevData() {

	for {
		var revData string
		if ServiceClient != nil && ServiceClient.logined == true {
			//循环接收数据
			//接收数据
			data := ServiceClient.ReadPacket(ServiceClient.session)
			revData = ByteToString(data)
			//解析数据，接收数据为空时不做处理
			if revData != "" {
				JxRevData(data)
			}
		}
		time.Sleep(sleep)
	}
}

// <summary>
// 线程定时发送数据
// </summary>
// <param name=""></param>
// <returns></returns>
func SendData() {

	var str string

	//create timer
	timer := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-timer.C:
			if ServiceClient != nil && ServiceClient.logined == true {
				//定时发送心跳
				heartBeatInfo := &ServiceInfo{}
				heartBeatInfo.Type = "heartbeat"
				heartBeatInfo.Msg.Service_code = ServiceCodeInfo

				str = heartBeatInfo.JsonConfigInfo()
				ClientSendData(str)

				//ServiceLastTime = time.Now().Format("2006-01-02 15:04:05")
				upLoadFLag = true
			}
		}

	}
}

// <summary>
// 线程发送注册信息
// </summary>
// <param name=""></param>
// <returns></returns>
func SendRegisterInfo() {

	var str string

	//create timer
	timer := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-timer.C:
			if registerFlag == false {
				//离线退出循环
				if ServiceClient.IsLogin() == false {
					break
				}
				//当连接对象为true时发送数据
				if ServiceClient.logined == true {
					//发送注册信息
					str = RegisterBaseInfo.JsonConfigInfo()
					ClientSendData(str)
				}
			} else if registerFlag == true {
				break
			}
		case <-time.After(240 * time.Second): //四次自动退出
			break
		}

	}

}

// <summary>
// 解析接收到的数据
// </summary>
// <param name="data">接收到的数据</param>
// <returns></returns>
func JxRevData(data []byte) {

	var code, msg string
	var err error
	//判断是否为断包数据
	if AppendStr != "" {
		var buff bytes.Buffer
		buff.WriteString(AppendStr)
		buff.WriteString(string(data))
		AppendStr = ""
		data = make([]byte, 0)
		data = buff.Bytes()
	}
	strData := string(data)
	strDatas := strings.FieldsFunc(strData, IsSlash)
	for _, jsonStr := range strDatas {

		if jsonStr == "" {
			continue
		}

		//截取json串
		strData := Substr2(jsonStr, 4, len(jsonStr))
		//fmt.Println(strData)
		revData, errs := jason.NewObjectFromBytes([]byte(strData))
		if errs != nil {
			AppendStr = jsonStr
		} else {
			code, err = revData.GetString("type")
			if err == nil {
				switch strings.ToLower(code) {
				case "heartbeat": //心跳
					msg, err = revData.GetString("message")
					if err == nil {
						if strings.ToLower(msg) == "receive" {
							//ServiceLastTime = time.Now().Format("2006-01-02 15:04:05")
							ServiceLastTime = timebase.NowTimeFormat()
							upLoadFLag = false
						}
					}
				case "register": //注册
					msg, err = revData.GetString("message")
					if err == nil {
						switch strings.ToLower(msg) {
						case "receive": //收到注册指令
							errorlog.ErrorLogDebug("command", "命令-接收", "监控中心回复注册")
						case "success": //注册成功
							registerFlag = true
							errorlog.ErrorLogDebug("command", "命令-接收", "服务注册成功")
						case "failed": //注册失败
							errorlog.ErrorLogDebug("command", "命令-接收", "服务注册失败\n")
						}
					}
				case "config": //配置信息
					//更新配置文件
					ConfigBaseInfo.Service_name, _ = revData.GetString("message", "service_name")
					ConfigBaseInfo.Service_code, _ = revData.GetString("message", "service_code")
					ConfigBaseInfo.Service_ip, _ = revData.GetString("message", "service_ip")
					ConfigBaseInfo.Service_port, _ = revData.GetString("message", "service_port")
					ConfigBaseInfo.Soft_version, _ = revData.GetString("message", "soft_version")
					ConfigBaseInfo.Watchdog_code, _ = revData.GetString("message", "watchdog_code")
					ConfigBaseInfo.Service_addr, _ = revData.GetString("message", "service_addr")
					ConfigBaseInfo.Servicetype_code, _ = revData.GetString("message", "servicetype_code")
					ConfigBaseInfo.Protocaltype_code, _ = revData.GetString("message", "protocaltype_code")

					//解析数据库数组
					dataArray, _ := revData.GetObjectArray("message", "db")
					for index, da := range dataArray {
						ConfigBaseInfo.Db[index].Db_type, _ = da.GetString("db_type")
						ConfigBaseInfo.Db[index].Db_ip, _ = da.GetString("db_ip")
						ConfigBaseInfo.Db[index].Db_port, _ = da.GetString("db_port")
						ConfigBaseInfo.Db[index].Db_instance, _ = da.GetString("db_instance")
						ConfigBaseInfo.Db[index].Db_user, _ = da.GetString("db_user")
						ConfigBaseInfo.Db[index].Db_password, _ = da.GetString("db_password")
						ConfigBaseInfo.Db[index].Data_type, _ = da.GetString("data_type")
						ConfigBaseInfo.Db[index].Remark, _ = da.GetString("remark")
					}

					//返回命令
					BackReplyInfo(1, "config", "", "receive")
					errorlog.ErrorLogDebug("command", "命令-接收", "修改配置文件")
					//更新执行后返回命令

					if ok := SetIniConfig(ConfigBaseInfo); ok == nil {
						BackReplyInfo(1, "config", "", "success")
						errorlog.ErrorLogDebug("command", "命令-发送", "配置文件修改成功\n")
					} else {
						BackReplyInfo(1, "config", "", "failed")
						errorlog.ErrorLogDebug("command", "命令-发送", "配置文件修改失败\n")
					}

				case "monitor": //服务监测
					//启动异常子服务
					var info = new(ServerInfo)

					serviceCode, err := revData.GetString("message", "service_code")
					//添加服务编码到map中
					if err == nil {
						SubServices[serviceCode] = info

						//返回命令
						BackReplyInfo(1, "monitor", "", "receive")
						errorlog.ErrorLogDebug("command", "命令-接收", "服务监测,监控服务编码="+serviceCode)
					}
					//读取下发状态
					serviceState, err := revData.GetString("message", "service_state")
					if err == nil {
						info.State, err = strconv.Atoi(serviceState)
						if err != nil {
							errorlog.ErrorLogError("command", "命令-解析失败",fmt.Sprintf("服务编码=%s,服务状态=%s",serviceCode,serviceState))
							continue
						}
					}
					serviceAddr, err := revData.GetString("message", "service_addr")
					//先判断平台下发服务状态，如果为启动状态则对服务启动
					if err == nil && info.State == 1 {

						switch SysType {
						case "linux":
							sys := new(LinuxSys)
							//重启服务进程
							err = sys.ProcessRestart(serviceAddr)
						case "windows":
							sys := new(WindowsSys)
							//重启服务进程
							err = sys.ProcessRestart(serviceAddr)
						}

						if err == nil {
							BackReplyInfo(1, "monitor", "", "success")
							errorlog.ErrorLogDebug("command", "命令-发送", fmt.Sprintf("服务重启成功,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
						} else {
							BackReplyInfo(1, "monitor", "", "failed")
							errorlog.ErrorLogDebug("command", "命令-发送",fmt.Sprintf("服务重启失败,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
						}
					}
				case "start": //开启服务
					//启动子服务
					var info = new(ServerInfo)

					serviceCode, err := revData.GetString("message", "service_code")
					//添加服务编码到map中
					if err == nil {
						SubServices[serviceCode] = info
					}
					serviceAddr, err := revData.GetString("message", "service_addr")

					//先判断是否已经启动，然后在启动
					if err == nil && serviceAddr != "" {
						//返回命令
						BackReplyInfo(2, "start", serviceCode, "receive")
						errorlog.ErrorLogDebug("command", "命令-接收", "启动服务,服务编码="+serviceCode)

						switch SysType {
						case "linux":
							sys := new(LinuxSys)
							//重启服务进程
							err = sys.ProcessRestart(serviceAddr)
						case "windows":
							sys := new(WindowsSys)
							//重启服务进程
							err = sys.ProcessRestart(serviceAddr)
						}

						if err == nil {
							BackReplyInfo(2, "start", serviceCode, "success")
							errorlog.ErrorLogDebug("command", "命令-发送", fmt.Sprintf("服务启动成功,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
						} else {
							BackReplyInfo(2, "start", serviceCode, "failed")
							errorlog.ErrorLogDebug("command", "命令-发送", fmt.Sprintf("服务启动失败,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
						}
					} else {
						errorlog.ErrorLogDebug("command", "命令-发送", fmt.Sprintf("服务停止失败,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
					}

				case "stop": //关闭服务

					//关闭子服务
					serviceCode, err := revData.GetString("message", "service_code")
					//添加服务编码到map中
					if err == nil {
						//返回命令
						BackReplyInfo(2, "stop", serviceCode, "receive")
						errorlog.ErrorLogDebug("command", "命令-接收", fmt.Sprintf("停止服务,服务编码=%s",serviceCode))

						serviceAddr, err := revData.GetString("message", "service_addr")
						//先判断是否已经启动，然后在启动
						if err == nil {

							switch SysType {
							case "linux":
								sys := new(LinuxSys)
								//重启服务进程
								err = sys.ProcessStop(serviceAddr)
							case "windows":
								sys := new(WindowsSys)
								//重启服务进程
								err = sys.ProcessStop(serviceAddr)
							}

							if err == nil {
								BackReplyInfo(2, "stop", serviceCode, "success")
								errorlog.ErrorLogDebug("command", "命令-发送", fmt.Sprintf("服务停止成功,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
							} else {
								BackReplyInfo(2, "stop", serviceCode, "failed")
								errorlog.ErrorLogDebug("command", "命令-发送", fmt.Sprintf("服务停止失败,服务编码=%s,服务地址=%s",serviceCode,serviceAddr))
							}

							//删除map中的子服务站点
							delete(SubServices, serviceCode)
						}

					}
				}
			}
		}
	}
}

// <summary>
// 检测离线函数
// </summary>
// <param name="wg">线程</param>
// <returns></returns>
func CheckClientDown() {

	var count int64
	count = 10
	for {
		t_time, err := time.Parse("2006-01-02 15:04:05", ServiceLastTime)
		if err != nil {
			continue
		}
		now_time, err := time.Parse("2006-01-02 15:04:05", time.Now().Format("2006-01-02 15:04:05"))

		diffTime := now_time.Sub(t_time).Seconds()

		//判断离线
		if ServiceClient != nil && ServiceClient.logined == true {
			if upLoadFLag == true {
				//没有收到回复时，监测离线重新链接
				if count > 30 {
					//退出连接
					count = 10
					ServiceClient.Logout()
					ServiceLastTime = time.Now().Format("2006-01-02 15:04:05")
				}
				//连续发送3次心跳
				if int64(diffTime) >= count {
					//定时发送心跳
					heartBeatInfo := &ServiceInfo{}
					heartBeatInfo.Type = "heartbeat"
					heartBeatInfo.Msg.Service_code = ServiceCodeInfo

					str := heartBeatInfo.JsonConfigInfo()
					ClientSendData(str)

					upLoadFLag = true
					count = count + 10
				}
			} else if diffTime >= Uploadinterval {
				//log.Println("超时重新连接")
				errorlog.ErrorLogDebug("client","超时","超时重新连接")
				ServiceClient.Logout()
				ServiceLastTime = time.Now().Format("2006-01-02 15:04:05")
			}
		}
		//延迟1S
		time.Sleep(sleep)
	}
}

// <summary>
// 返回平台命令
// </summary>
// <param name="backType">返回类型 1:配置、监测 2：启动、停止</param>
// <param name="agreeName">协议编码</param>
// <param name="code">服务编码</param>
// <param name="result">状态</param>
// <returns></returns>
func BackReplyInfo(backType int, agreeName, code, result string) {

	var str string
	if backType == 1 {
		backStr := &ReplyInfo{Type: agreeName, Msg: result} //failed
		str = backStr.JsonConfigInfo()
	} else if backType == 2 {
		backStr := &ReplyStartStopInfo{Type: agreeName, Service_code: code, Msg: result} //failed
		str = backStr.JsonConfigInfo()
	}
	if str != "" {
		ServiceClient.SendMessage(FomateProtocol(str))
	}
}

// <summary>
// 格式化协议报文
// </summary>
// <param name="str">报文</param>
// <returns></returns>
func FomateProtocol(str string) string {
	return fmt.Sprintf("##%04d%s**", len([]byte(str)), str)
}

// <summary>
// 发送数据
// </summary>
// <param name="str">报文</param>
// <returns></returns>
func ClientSendData(data string) {
	if data != "" {
		if flag := ServiceClient.SendMessage(FomateProtocol(data)); flag == false {
			//log.Println("发送失败!发送数据:" + data)
		}
	}
}
