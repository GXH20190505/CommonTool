package errorlog

import (
	"path"
	"runtime"
	"testing"
)

func TestErrorLogInfo(t *testing.T) {
	ErrorLogInfo("test","运行错误","错误信息")
}
func TestInitErrorlog(t *testing.T) {
	InitErrorlog("DEBUG",3)
}
func TestErrorLogError(t *testing.T) {
	ErrorLogError("test","Error","string")
}
func TestErrorLogDebug(t *testing.T) {
	ErrorLogDebug("test","Debug","string")
}
func TestErrorLogWarn(t *testing.T) {
	t.Run("logWarn", func(t *testing.T) {
		ErrorLogWarn("test","Warn","string")
	})

}
func TestRemoveDir(t *testing.T) {
	RemoveDir()//删除前三天日志
}
func getCurrentPath() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Dir(filename)
}

