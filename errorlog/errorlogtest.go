package errorlog
func errlogTest() {
 //test 文件名称，name事件名称，string事件内容
 //logleve 1 debug,2 info,3 warn,4 error
 ErrorLogInfo("test","Info","string")
 InitErrorlog("DEBUG",3)
 ErrorLogError("test","Error","string")
 ErrorLogDebug("test","Debug","string")
 ErrorLog(2,"test","log","string")
 ErrorLogWarn("test","Warn","string")
 RemoveDir()//删除前三天日志
}
