package main 


/** 测试说明
	1、删除所有文件
	2、创建2个基础文件 ./test/a.txt ./test/b.txt
	3、创建压缩文件夹 ./com
	4、创建解压缩文件夹 ./uncom
	5、挨个验证各压缩、解压缩指令
	6、验证文件
*/

import (
	"fmt"
	"runtime"
	"os"
	
	"filebase"
	"compressbyshell"
)

const (
	baseA = iota
	baseB 
	zipAddr
	zipAddrAll
	targzAddr
	targzAddrAll 
	tarbz2Addr
	tarbz2AddrAll 
	uncomZipA
	uncomZipAll 
	untargzA 
	untargzAll 
	untarbz2A 
	untarbz2All 
)

//文件地址
var filePaths = []string {
	baseA 			: "./test/a.txt",
	baseB 			: "./test/b.txt",

	zipAddr 		: "./com/a.zip",
	zipAddrAll 	: "./com/all.zip",

	targzAddr 		: "./com/a.tar.gz",
	targzAddrAll 	: "./com/all.tar.gz",

	tarbz2Addr 		: "./com/a.tar.bz2",
	tarbz2AddrAll 	: "./com/all.tar.bz2",

	uncomZipA 		: "./unzip/",
	uncomZipAll 	: "./upzip2/",

	untargzA 		: "./untargz/a.txt",
	untargzAll 		: "./untargz2/",
	
	untarbz2A 		: "./untarbz/a.txt",
	untarbz2All 	: "./untarbz2/",
}

/*
//文件地址
baseA := "./test/a.txt"
baseB := "./test/b.txt"

zipAddr := "./com/a.zip"
zipAddrALll := "./com/all.zip"

targzAddr := "./com/a.tar.gz"
targzAddrAll := "./com/all.tar.gz"

tarbz2Addr := "./com/a.tar.bz2"
tarbz2AddrAll := "./com/all.tar.bz2"

uncomZipA := "./unzip/a.txt"
uncomZipAll := "./upzip2/"

untargzA := "./untargz/a.txt"
untargzAll := "./untargz2/"
	
untarbz2A := "./untarbz/a.txt"
untarbz2All := "./untarbz2/"
*/
func testAll(){
	//1、先检查所有文件夹
	//2、删除文件
	for _,strs := range filePaths {
		filebase.GetFilePath(strs)
		if filebase.CheckFileIsExist(strs) {
			os.Remove(strs)
		}
	}
	//生成基础文件
	filebase.WriteData(filePaths[baseA],"hello world")
	filebase.WriteData(filePaths[baseB],"nice to meet you")
	
	//3、先压缩文件
	var comShell compressbyshell.Compress
	
	if runtime.GOOS == "windows" {
		var temp compressbyshell.Compress = new(compressbyshell.CompressWin)
		comShell = temp
	}else if runtime.GOOS == "linux" {
		var temp compressbyshell.Compress = new(compressbyshell.CompressLinux)
		comShell = temp
	}
	
	//测试压缩
	err :=  comShell.CompressZip(filePaths[zipAddr],filePaths[baseA])
	fmt.Println("ZIP:",err)
	
	strs := []string{filePaths[baseA],filePaths[baseB]}
	err = comShell.CompressZipAll(filePaths[zipAddrAll],strs)
	fmt.Println("ZIP_ALL:",err)
	
	err = comShell.CompressTarGzAll(filePaths[targzAddrAll],strs)
	fmt.Println("Tar.gz",err)
	
	err = comShell.CompressTarBzip2All(filePaths[tarbz2AddrAll],strs)
	fmt.Println("Tar.bz2",err)
	
	//测试解压缩
	err = comShell.UncompressZip(filePaths[zipAddr],filePaths[uncomZipA])
	fmt.Println("UnZip:",err)
	if err!=nil {
		fmt.Println(filePaths[zipAddr],filePaths[uncomZipA])
	}
	
	err = comShell.UncompressTarGz(filePaths[targzAddrAll],filePaths[untargzAll])
	fmt.Println("UnTarGz:",err)
}
