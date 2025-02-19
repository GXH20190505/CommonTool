package errorlog

import (
	"filebase"
	"fmt"
	"log"
	"os"
	"runtime"

	"strings"
	"sync"
	"time"
)

///新建日志创建包
//主要功能
//1、按名称保存日志文件
//2、按级别保存日志文件
//3、保存日志格式为[时间][级别][日志名称][详细信息]

const (
	_ = iota
	LEVEL_DEBUG
	LEVEL_INFO
	LEVEL_WARN
	LEVEL_ERROR
)

var _LevelName = []string{"", "DEBUG", "INFO", "WARN", "ERROR"}
var logLevel int = LEVEL_DEBUG
var logKeep int = 3
var logLock sync.Mutex

/** 初始化日志打印
 */
func InitErrorlog(LogLevel string, LogKeep int) {
	switch LogLevel {
	case "DEBUG":
		logLevel = LEVEL_DEBUG
	case "INFO":
		logLevel = LEVEL_INFO
	case "WARN":
		logLevel = LEVEL_WARN
	case "ERROR":
		logLevel = LEVEL_ERROR
	default:
		logLevel = LEVEL_INFO
	}
	logKeep = LogKeep
}

func ErrorLogDebug(mode string, name string, msg string) {
	ErrorLog(LEVEL_DEBUG, mode, name, msg)
}
func ErrorLogInfo(mode string, name string, msg string) {
	ErrorLog(LEVEL_INFO, mode, name, msg)
}
func ErrorLogWarn(mode string, name string, msg string) {
	ErrorLog(LEVEL_WARN, mode, name, msg)
}
func ErrorLogError(mode string, name string, msg string) {
	ErrorLog(LEVEL_ERROR, mode, name, msg)
}

func ErrorLog(level int, mode string, name string, msg string) {
	logLock.Lock()
	defer logLock.Unlock()
	if level < logLevel || level > LEVEL_ERROR {
		return
	}
	logPath, _ := createlogdir()
	if mode == "" {
		mode = "system"
	}
	logfile, err := os.OpenFile(logPath+"/"+mode+".log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	os.Chmod(logPath+"/"+mode+".log", 0777)
	defer logfile.Close()

	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		return
	}
	//logger := log.New(logfile, "\r\n", log.Ldate|log.Lmicroseconds|log.Lshortfile|log.LstdFlags)
	f := Caller(3)
	logger := log.New(logfile, "\r\n", log.Ldate|log.Lmicroseconds)
	strMsg := fmt.Sprintf("[%s][%s]%s  %s", _LevelName[level], name, f, msg)
	logger.Println(strMsg)
	if len(strMsg)>102400{
		return
	}
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.999"), strMsg)
}

//根据日期生成文件夹
func createlogdir() (string, error) {

	path := filebase.GetLocalPath()
	pathDir, _ := filebase.GetFilePath(path)

	//获取当前程序路径
	var sPath = fmt.Sprintf("%vLog/%v", pathDir, time.Now().Format("20060102"))

	err := os.MkdirAll(sPath, 0777)
	os.Chmod(sPath, 0777)
	return sPath, err
}

/*
删除历史3天前日志目录
*/
func RemoveDir() {
	
	path := filebase.GetLocalPath()
	pathDir, _ := filebase.GetFilePath(path)

	timet := time.NewTicker(5 * time.Minute)
	ErrorLog(LEVEL_INFO, "system", "日志", "日志清理线程启动")
	for {
		
		select {
		case <-timet.C:
			//获取当前程序路径
			var tKeep time.Duration
			tKeep = time.Duration(logKeep) * time.Hour * 24
			var sPath = fmt.Sprintf("%vLog/%v", pathDir, (time.Now().Add(-tKeep).Format("20060102")))
			//
			if ok := filebase.CheckFileIsExist(sPath); ok {
				err := os.RemoveAll(sPath)
				if err != nil {
					log.Println(fmt.Sprintf("删除历史日志失败%v", sPath))
				}
			}
		}
	}
}

/**
runtime.Caller(skip int)(pc uintptr,file string,line int,ok bool)
参数
skip:要提升堆栈帧数，0=当前函数，1=上一层函数。。。
	 使用时需注意返回哪一层函数的地址
pc:函数指针
file:函数所在文件名地址（绝对地址）
line:函数所在行数
ok:是否可以获取到信息
*/

func Caller(depth int) string {
	_, file, line, _ := runtime.Caller(depth)
	fmt.Println(file, line)
	idx := strings.LastIndexByte(file, '/')
	return fmt.Sprintf("[%s:%d]", file[idx+1:], line)
}
