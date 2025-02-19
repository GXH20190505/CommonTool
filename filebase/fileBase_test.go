package filebase

import (
	"fmt"
	"os"
	"testing"
)
func creatFile()string{
	filepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\testdata.txt"
	if CheckFileIsExist(filepath){
		return filepath
	}
	_,err:=os.Create(filepath)
	if err!=nil{
		fmt.Println("文件创建失败")
		return ""
	}
	return  filepath
}
func crateFolder()string  {
	folderpath:= "C:\\Users\\Administrator\\Desktop\\TestFile"
	if CheckFileIsExist(folderpath){
		return folderpath
	}
	err:=CreateDir(folderpath)
	if err!=nil{
		return ""
	}
	return folderpath
}
func TestCheckFloderIsExist(t *testing.T) {
	strPath:= crateFolder()
	if strPath!="" {
		resultExit:= CheckFileIsExist(strPath)
		t.Logf("文件是否存在%v,路径%s",resultExit,strPath)
	}

}
func TestCheckFileIsExist(t *testing.T) {
	strPath:= creatFile()
	if strPath!="" {
		resultExit:= CheckFileIsExist(strPath)
		t.Logf("文件是否存在%v,路径%s",resultExit,strPath)
	}

}
func TestGetLocalPath(t *testing.T) {
		respath:= GetLocalPath()
		t.Log("文件路径",respath)
}
func TestReadData(t *testing.T) {
	respath:= creatFile()
	filedata:=ReadData(respath)
	t.Log(filedata)
}
/*写入文件，会替换掉旧有数据，不是在后面追加*/
func TestWriteData(t *testing.T) {
	respath:= creatFile()
	filedata:=WriteData(respath,"abcde")
	t.Log("写入数据",filedata)
}
func TestWriteDataByte(t *testing.T) {
	respath:= creatFile()
	bytData:=make([]byte,2)
	bytData[0]=0x11
	bytData[1]=0x10
	resultWriteByte:=WriteDataByte(respath,bytData)
	if !resultWriteByte{
		t.Log("byte写入失败")
	}
	t.Log("byte写入成功")
}

/*写入文件，在后面追加写入*/
func TestAppendDataByte(t *testing.T) {
	bytData:=make([]byte,2)
	bytData[0]=0x11
	bytData[1]=0x10
	respath:= creatFile()
	filedata:=AppendDataByte(respath,bytData)
	t.Log("写入数据",filedata)
}
/*	获取文件列表*/
func TestGetFileList(t *testing.T) {
	respath:=crateFolder()
	floderlen:= GetFileList(respath)
	for _, s := range floderlen {
		t.Log("文件名为",s)
	}
}
/*
fileAddr=文件本地路径
	sIndex=获取序号，sIndex 从1开始，每次返回1024字节数据*/
func TestGetFileData(t *testing.T) {
	respath:=creatFile()
	resultByte,_:=GetFileData(respath,1)
	t.Log("文件内容为",resultByte)
}

func TestGetFileDataAll(t *testing.T) {
	respath:=creatFile()
	filelen,_:= GetFileDataAll(respath)
	t.Log("文件内容为",filelen)
}

func TestGetFileLens(t *testing.T) {
	respath:=creatFile()
	filelen,_:= GetFileLens(respath)
	t.Log("文件大小为",filelen)
}
func TestCopyFile(t *testing.T) {
	sourfilepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\testdata.txt"
	copyfilepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\testdatacopy.txt"
   resInt,_:=CopyFile(sourfilepath,copyfilepath)
   t.Log("拷贝文件",resInt)
}
func TestDelFile(t *testing.T) {
	copyfilepath:= "C:\\Users\\Administrator\\Desktop\\TestFile\\testdatacopy.txt"
	err:= DelFile(copyfilepath)
	if err!=nil{
		t.Log("删除失败",err)
	}
	t.Log("删除成功")
}

