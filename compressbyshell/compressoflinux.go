package compressbyshell

import (
	"fmt"
	"os/exec"
	"filebase"
)

type CompressLinux struct {
	name string
}

func (self *CompressLinux)Init() error{
	return exec.Command("HaoZipC").Run() 
}

/** zip格式压缩 -- 单文件压缩
	zipAddr  == 压缩后的zip文件地址，绝对路径，包括文件名
	FileAddr == 压缩文件地址，绝对路径，包括文件名 也可以使用文件夹的绝对路径，表示压缩整个文件夹
	注意：假如zipAddr不带扩展名，则默认添加".zip"作为扩展名
	举例 zipAddr := "D:\1234.zip"
		FileAddr := "D:\1234.txt" or "D:\update"
*/
func (self *CompressLinux)CompressZip(zipAddr string ,fileAddr string) error {
	//压缩文件
	return exec.Command("zip", zipAddr, fileAddr).Run() 
	
}
/** zip格式压缩 -- 多文件压缩
	
*/
func (self *CompressLinux)CompressZipAll(zipAddr string,fileAddr []string) error {
	for _ ,fileA := range fileAddr {
		err := exec.Command("zip", zipAddr, fileA).Run()
		if err != nil {
			return err
		}
	}
	return nil
}



/** zip格式解压缩
	zipAddr  == 需要解压缩的zip文件地址，绝对路径，包括扩展名
	FileAddr == 解压的文件名，如果为空，则默认解压到zip文件目录下
	注：如果FileAddr为文件夹名，需要在最后加"/"
*/
func (self *CompressLinux)UncompressZip(zipAddr string,fileAddr string) error {
	if ok := filebase.CheckFileIsExist(zipAddr); ok==false {
		return fmt.Errorf("Zip file not found")
	}
	//解压缩文件
	return exec.Command("unzip", zipAddr, fileAddr).Run()	//注-o命令必须放在替换文件夹之前 并且没有空格
}


/** tar.gz格式压缩
*/
func (self *CompressLinux)CompressTarGzAll(tarAddr string, fileAddr []string) error {
	for _ ,fileA := range fileAddr {
		err:= exec.Command("tar","-zcvf", tarAddr, fileA).Run()
		if err!=nil {
			return err
		}
	}
	return nil
}

/** tar.gz格式解压缩
	tarAddr  == 需要解压缩的tar.gz文件地址，绝对路径，包括扩展名
	FileAddr == 解压的文件名，如果为空，则默认解压到tar.gz文件目录下
	注：如果FileAddr为文件夹名，需要在最后加"/"
*/
func (self *CompressLinux)UncompressTarGz(tarAddr string,fileAddr string) error {
	if ok := filebase.CheckFileIsExist(tarAddr); ok==false {
		return fmt.Errorf("tar.gz file not found")
	}
	//解压缩文件
	return exec.Command("tar","-zvf", tarAddr, fileAddr).Run()	//注-o命令必须放在替换文件夹之前 并且没有空格
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
func (self *CompressLinux)CompressTarBzip2(tarAddr string, FileAddr string) error {
	if err := exec.Command("tar","-jcvf", tarAddr, FileAddr).Run();err != nil {
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
func (self *CompressLinux)CompressTarBzip2All(tarAddr string, FileAddr []string) error {
	for _,fName := range FileAddr {
		if err := exec.Command("tar","-jcvf", tarAddr, fName).Run();err != nil {
			return err
		}
	}
	return nil
}
