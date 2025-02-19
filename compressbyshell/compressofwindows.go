package compressbyshell

import (
	"strings"
	"fmt"
	"os/exec"
	"os"
	"filebase"
)

type CompressWin struct {
	name string
}

func (self *CompressWin)Init() error{
	return exec.Command("HaoZipC").Run() 
}

/** zip格式压缩 -- 单文件压缩
	zipAddr  == 压缩后的zip文件地址，绝对路径，包括文件名
	FileAddr == 压缩文件地址，绝对路径，包括文件名 也可以使用文件夹的绝对路径，表示压缩整个文件夹
	注意：假如zipAddr不带扩展名，则默认添加".zip"作为扩展名
	举例 zipAddr := "D:\1234.zip"
		FileAddr := "D:\1234.txt" or "D:\update"
*/
func (self *CompressWin)CompressZip(zipAddr string ,FileAddr string) error {
	/*
	if ok := filebase.CheckFileIsExist(FileAddr);ok==false {
		return fmt.Errorf("File not found")
	}*/
	//压缩文件
	return exec.Command("HaoZipC","a","-y","-tzip",ForWard_To_BackSlash(zipAddr),ForWard_To_BackSlash(FileAddr)).Run() 
	
}
/** zip格式压缩 -- 多文件压缩
	
*/
func (self *CompressWin)CompressZipAll(zipAddr string,FileAddr []string) error {
	for _ ,fileA := range FileAddr {
		err:= exec.Command("HaoZipC","a","-y","-tzip",ForWard_To_BackSlash(zipAddr),ForWard_To_BackSlash(fileA)).Run()
		if err!=nil {
			return err
		}
	}
	return nil
}



/** zip格式解压缩
	zipAddr  == 需要解压缩的zip文件地址，绝对路径，包括扩展名
	FileAddr == 解压所有文件放到此文件的目录下
	注：如果FileAddr为文件夹名，需要在最后加"/"
*/
func (self *CompressWin)UncompressZip(zipAddr string,FileAddr string) error {
	if ok := filebase.CheckFileIsExist(zipAddr); ok==false {
		return fmt.Errorf("Zip file not found")
	}
	//解压缩文件
	fPath,_:= filebase.GetFilePath(FileAddr)
	return exec.Command("HaoZipC","e","-y",zipAddr,"-o"+fPath).Run()	//注-o命令必须放在替换文件夹之前 并且没有空格
}


/** tar.gz格式压缩
*/
func (self *CompressWin)CompressTarGzAll(tarAddr string, FileAddr []string) error {
	tarName :=strings.Replace(tarAddr,".tar.gz",".tar",-1)//tar包名称转换，先换成tar包名称
	for _,fName := range FileAddr {
		if err := exec.Command("HaoZipC","a","-y","-ttar",ForWard_To_BackSlash(tarName),"-w",ForWard_To_BackSlash(fName)).Run();err != nil {
			return err
		}
	}
	if filebase.CheckFileIsExist(tarAddr){
		os.Remove(tarAddr) 
	}
	if err := exec.Command("HaoZipC","a","-y","-tgzip",ForWard_To_BackSlash(tarAddr),"-w",(tarName)).Run();err!=nil {
		return err
	}
	return nil
}

/** tar.gz格式解压缩
	tarAddr  == 需要解压缩的tar.gz文件地址，绝对路径，包括扩展名
	FileAddr == 解压的文件名，如果为空，则默认解压到tar.gz文件目录下
	注：如果FileAddr为文件夹名，需要在最后加"/"
	只支持解压到对应的文件夹下，不支持选择文件
*/
func (self *CompressWin)UncompressTarGz(tarAddr string,FileAddr string) error {
	if ok := filebase.CheckFileIsExist(tarAddr); ok==false {
		return fmt.Errorf("tar.gz file not found")
	}
	//解压缩文件
	fPath,_:= filebase.GetFilePath(FileAddr)
	return exec.Command("HaoZipC","e","-y",tarAddr,"-o" + fPath).Run()	//注-o命令必须放在替换文件夹之前 并且没有空格
}

/** tar.bz2 格式压缩
	步骤
	1、先压缩成tar包格式
	2、把tar包压缩成bz2格式
	3、注意：假如已有该文件，则会报错，需要先删除该tar.bz2文件
	参数
	zipAddr  == 压缩后的tar.bz2文件地址，绝对路径，包括文件名,文件名必须为*.tar.bz2
	FileAddr == 压缩文件地址，绝对路径，包括文件名 每个变量都必须是绝对路径
*/
func (self *CompressWin)CompressTarBzip2(tarAddr string, FileAddr string) error {
	tarName :=strings.Replace(tarAddr,".tar.bz2",".tar",-1)//tar包名称转换，先换成tar包名称
	//先删除tar包，防止文件没有更新
	if filebase.CheckFileIsExist(tarName){
		os.Remove(tarAddr) 
	}
	if err := exec.Command("HaoZipC","a","-y","-ttar",ForWard_To_BackSlash(tarName),"-w",ForWard_To_BackSlash(FileAddr)).Run();err != nil {
		return err
	}

	if filebase.CheckFileIsExist(tarAddr){
		os.Remove(tarAddr) 
	}
	if err := exec.Command("HaoZipC","a","-y","-tbzip2",ForWard_To_BackSlash(tarAddr),"-w",(tarName)).Run();err!=nil {
		return err
	}
	return nil
}

/** tar.bz2 格式压缩
	步骤
	1、先压缩成tar包格式
	2、把tar包压缩成bz2格式
	3、注意：假如已有该文件，则会报错，需要先删除该tar.bz2文件
	参数
	zipAddr  == 压缩后的tar.bz2文件地址，绝对路径，包括文件名,文件名必须为*.tar.bz2
	FileAddr == 压缩文件地址，绝对路径，包括文件名 每个变量都必须是绝对路径
*/
func (self *CompressWin)CompressTarBzip2All(tarAddr string, FileAddr []string) error {
	tarName :=strings.Replace(tarAddr,".tar.bz2",".tar",-1)//tar包名称转换，先换成tar包名称
	for _,fName := range FileAddr {
		if err := exec.Command("HaoZipC","a","-y","-ttar",ForWard_To_BackSlash(tarName),"-w",ForWard_To_BackSlash(fName)).Run();err != nil {
			return err
		}
	}
	if filebase.CheckFileIsExist(tarAddr){
		os.Remove(tarAddr) 
	}
	if err := exec.Command("HaoZipC","a","-y","-tbzip2",ForWard_To_BackSlash(tarAddr),"-w",(tarName)).Run();err!=nil {
		return err
	}
	return nil
}

/** 正反斜杠替换
	文件地址中，需要将正斜杠(forward slash"/")转换为反斜杠(back slash "\")
	否则部分指令操作会出现无法找到文件夹或地址的错误
*/
func ForWard_To_BackSlash(sAddr string)string{
	return strings.Replace(sAddr,"/","\\",-1)
}