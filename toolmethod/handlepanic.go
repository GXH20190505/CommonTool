package toolbase
import (
	"fmt"
	"runtime"
	"errorlog"
)
func HandlePanic(funcName string) {
	if r := recover(); r != nil {
		stackTrace := captureStacktrace(false)
		errorlog.ErrorLogError("panic", funcName, fmt.Sprintf("panic:%v stack:%v",r , stackTrace))
	} else {
		errorlog.ErrorLogDebug("panic", funcName, fmt.Sprintf("正常退出"))
	}
}
func captureStacktrace(all bool) string {
	const initialSize = 512
	buff := make([]byte, initialSize)
	size := initialSize
	for {
		n := runtime.Stack(buff, all)
		if n < size {
			return string(buff[:n])
		}
		size *= 2 // Ensure size is an integer type
		buff = make([]byte, size)
	}
}
func HandlePanicFunc(funcName string) {
	if r := recover(); r != nil {
		stackTrace := captureStacktrace(false)
		errorlog.ErrorLogError("panic", funcName, fmt.Sprintf("panic:%v stack:%v",r , stackTrace))
	} else {
		errorlog.ErrorLogDebug("panic", funcName, fmt.Sprintf("正常退出"))
	}
}