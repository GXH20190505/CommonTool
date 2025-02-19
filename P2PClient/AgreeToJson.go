package P2PClient

import "encoding/json"

// <summary>
// 注册\配置信息转Json
// </summary>
// <param name="info">注册\配置信息struct</param>
// <returns>json数据</returns>
func (info *ConfigService) JsonConfigInfo() string {
	js, err := json.Marshal(info)
	if err != nil {
		panic(err)
		return ""
	}
	return string(js)
}

// <summary>
// 启动、关闭信息转Json
// </summary>
// <param name="info">启动、关闭信息struct</param>
// <returns>json数据</returns>
func (info *StartStopInfo) JsonConfigInfo() string {
	js, err := json.Marshal(info)
	if err != nil {
		panic(err)
		return ""
	}
	return string(js)
}

/********
心跳
********/
func (info *ServiceInfo) JsonConfigInfo() string {

	js, err := json.Marshal(info)
	if err != nil {
		panic(err)
		return ""
	}
	return string(js)
}

/********
心跳\注册、配置回复信息
********/
func (info *ReplyInfo) JsonConfigInfo() string {
	js, err := json.Marshal(info)
	if err != nil {
		panic(err)
		return ""
	}
	return string(js)
}

/********
启动、停止回复信息
********/
func (info *ReplyStartStopInfo) JsonConfigInfo() string {
	js, err := json.Marshal(info)
	if err != nil {
		panic(err)
		return ""
	}
	return string(js)
}
