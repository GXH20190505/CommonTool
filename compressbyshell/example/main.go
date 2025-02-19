package main

import (
	"fmt"
	
)


 func main() {
	
	testAll()
	
	fmt.Println("end")
	/*
	var comTemp compressbyshell.Compress = new(compressbyshell.CompressWin)
	
	comTemp.Init()
	*/
	/*
	if err := compressbyshell.InitOfWindows();err != nil {
		fmt.Println(err)
		return
	}*/
	/*
	compressbyshell.WinComressZip("D:/6379.zip","D:/6379.txt")
	
	var fileList []string
	fileList= append(fileList,"D:/6379.txt")
	fileList= append(fileList,"D:/7000.txt")
	compressbyshell.WinComressZipAll("D:/7000.zip",fileList)
	
	if err := compressbyshell.WinUncompressZip("D:/7000.zip","D:/wamp");err != nil {
		fmt.Println(err)
	}
	
	if err := compressbyshell.WinUncomressTarGz("D:/wamp/configure","D:/wamp/www");err != nil {
		fmt.Println(err)
	}
	*/
	/*
	var fileList []string
	fileList= append(fileList,"D:/6379.txt")
	fileList= append(fileList,"D:/7000.txt")
	if err := compressbyshell.WinComressTarBzip2All("D:/wamp/update.tar.bz2",fileList);err != nil {
		fmt.Println(err)
	}
	*/
}

