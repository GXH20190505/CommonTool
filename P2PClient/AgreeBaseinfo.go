package P2PClient

/*****************
协议内容
******************/

// <summary>
// 数据库参数
// </summary>
type DbT struct {
	Db_type     string `json:"db_type"`
	Db_ip       string `json:"db_ip"`
	Db_port     string `json:"db_port"`
	Db_instance string `json:"db_instance"`
	Db_user     string `json:"db_user"`
	Db_password string `json:"db_password"`
	Data_type   string `json:"data_type"`
	Remark      string `json:"remark"`
}

// <summary>
// 消息配置参数
// </summary>
type Message struct {
	Service_name      string `json:"service_name"`
	Service_code      string `json:"service_code"`
	Service_ip        string `json:"service_ip"`
	Service_port      string `json:"service_port"`
	Soft_version      string `json:"soft_version"`
	Watchdog_code     string `json:"watchdog_code"`
	Service_addr      string `json:"service_addr"`
	Servicetype_code  string `json:"servicetype_code"`
	Protocaltype_code string `json:"protocaltype_code"`
	Db                []*DbT `json:"db"`
}

// <summary>
// 注册、配置信息ConfigService
// </summary>
type ConfigService struct {
	Type    string `json:"type"`
	Message `json:"message"`
}

// <summary>
// 监控信息
// </summary>
type MonitortInfo struct {
	Type string `json:"type"`
	Msg  struct {
		Service_code  string `json:"service_code"`
		Watchdog_code string `json:"watchdog_code"`
		Service_state string `json:"service_state"`
		Service_Addr  string `json:"service_addr"`
	} `json:"message"`
}

// <summary>
// 启动、停止信息
// </summary>
type StartStopInfo struct {
	Type string `json:"type"`
	Msg  struct {
		Service_code  string `json:"service_code"`
		Watchdog_code string `json:"watchdog_code"`
		Service_Addr  string `json:"service_addr"`
	} `json:"message"`
}

// <summary>
// 心跳信息
// </summary>
type ServiceInfo struct {
	Type string `json:"type"`
	Msg  struct {
		Service_code string `json:"service_code"`
	} `json:"message"`
}

// <summary>
// 接收回复\执行信息
// </summary>
type ReplyInfo struct {
	Type string `json:"type"`
	Msg  string `json:"message"` //receive:成功接收 success/failed:执行成功和失败
}

// <summary>
// 接收启动、停止回复\执行信息
// </summary>
type ReplyStartStopInfo struct {
	Type         string `json:"type"`
	Service_code string `json:"service_code"`
	Msg          string `json:"message"` //receive:成功接收 success/failed:执行成功和失败
}
