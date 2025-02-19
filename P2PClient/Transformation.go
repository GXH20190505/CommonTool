package P2PClient

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

/*****************
数据的转换
******************/

// <summary>
// 字节数组转为字符串
// </summary>
// <param name="p">字节数组</param>
// <returns></returns>
func ByteToString(p []byte) string {
	for i := 0; i < len(p); i++ {
		if p[i] == 0 {
			return string(p[0:i])
		}
	}
	return string(p)
}

// <summary>
// 截取字符串(不包含最后一个字符)
// </summary>
// <param name="str">原字符串</param>
// <param name="start">起点下标</param>
// <param name="end">终点下标</param>
// <returns></returns>
func Substr2(str string, start int, end int) string {
	rs := []rune(str)
	length := len(rs)

	if start < 0 || start > length {
		panic("start is wrong")
	}

	if end < 0 || end > length {
		panic("end is wrong")
	}

	return string(rs[start:end])
}

// <summary>
// 字符串转日期格式
// </summary>
// <param name="str">字符串 例:20160303130000</param>
// <returns></returns>
func StrToTime(str string) (string, error) {
	if len(str) >= 16 {
		strs := []rune(str)
		t1 := fmt.Sprintf("%s-%s-%s %s:%s:%s", string(strs[0:4]), string(strs[4:6]), string(strs[6:8]), string(strs[8:10]), string(strs[10:12]), string(strs[12:14]))
		return t1, nil
	}
	return "", errors.New("转换出错")
}

// <summary>
// 获取路径
// </summary>
// <param name="str">文件完整路径名</param>
// <returns></returns>
func GetServerPath(path string) (string, error) {

	if path != "" {
		if l := strings.LastIndexFunc(path, IsPath); l > 0 {
			str := []rune(path)
			return string(str[:l+1]), nil
		}
	}
	return "", errors.New("转换出错")
}

func IsPath(r rune) bool {
	return r == '/' || r == '\\'
}
func IsSlash(r rune) bool {
	return r == '#' || r == '*'
}
func IsProcess(r rune) bool {
	return r == ';'
}

// <summary>
// 读取本地运行路径
// </summary>
// <returns></returns>
func GetLocalPath() (string, error) {
	var filePath string
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	filePath, err = filepath.Abs(file)
	if err != nil {
		return "", err
	}
	return filePath, nil
}

// <summary>
// 读取本地运行路径
// </summary>
// <param name="path">文件完整路径名</param>
// <returns></returns>
func IsExistPath(path string) bool {
	_, err := os.Stat(path)
	return err != nil || os.IsNotExist(err)
}

// <summary>
//
// </summary>
// <returns></returns>
func CreateIniDir() error {
	//获取当前程序路径
	var sPath = "config"

	err := os.MkdirAll(sPath, 0777)
	os.Chmod(sPath, 0777)
	if err != nil {
		return errors.New("111")
	}

	return nil
}

// <summary>
//判断文件夹是否存在,不存在则创建
// </summary>
// <param name="dirname">文件完整路径名</param>
// <returns></returns>
func CreateFile(filepath string) error {

	files, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	os.Chmod(filepath, 0777)
	defer files.Close()
	if err != nil {
		return err
	}

	return nil
}
