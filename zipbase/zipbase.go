package zipbase

import (
    "archive/zip"
    "fmt"
    "io"
    "io/ioutil"
    "os"
	"filebase"
	
)

/*
	文件压缩
	sourceName=源文件名称	带路径
	zipName=目标文件名称 带路径
*/
func CompressZip(sourceName string,zipName string)error {
	if zipName == "" {
		zipName = sourceName + ".zip"
	}
    fzip, _ := os.Create(zipName)
    w := zip.NewWriter(fzip)
    defer w.Close()
	//添加文件
	_,sName := filebase.GetFilePath(sourceName)
	fw, _ := w.Create(sName)
	filecontent,err := ioutil.ReadFile(sourceName)
    if err != nil {
		return err
    }
    _, err = fw.Write(filecontent)
    if err != nil {
       	return err
    }
	return nil
}

func CompressZipByte(sData []byte,sourceName string,zipName string)error{
	
	if zipName == ""{
		zipName = sourceName + ".zip"
	}
	
	fzip,err := os.Create(zipName)
	if err != nil{
		fmt.Println(err)
	}
	
	w := zip.NewWriter(fzip)
	defer w.Close()
	
	_,sName := filebase.GetFilePath(sourceName)
	fw, _ := w.Create(sName)
	_, err = fw.Write(sData)
    if err != nil {
       	fmt.Println(err)
    }
	
	return nil
}


func DeCompressZip(sourceName string,zipName string) {
    //const File = "img-50.zip"
    //const dir = "img/"
	
    //os.Mkdir(dir, 0777) //创建一个目录

    cf, err := zip.OpenReader(zipName) //读取zip文件
    if err != nil {
        fmt.Println(err)
    }
    defer cf.Close()
	
	_,sName := filebase.GetFilePath(sourceName)
	
    for _, file := range cf.File {
		if file.Name != sName{
			continue
		}
        rc, err := file.Open()
        if err != nil {
            fmt.Println(err)
        }

        f, err := os.Create(sourceName)
        if err != nil {
            fmt.Println(err)
        }
        defer f.Close()
        n, err := io.Copy(f, rc)
        if err != nil {
            fmt.Println(err)
        }
        fmt.Println(n)
		break
    }

}

func DeCompressZipAll(zipName string) {

    cf, err := zip.OpenReader(zipName) //读取zip文件
    if err != nil {
        fmt.Println(err)
    }
    defer cf.Close()
	
	
    for _, file := range cf.File {
        rc, err := file.Open()
        if err != nil {
            fmt.Println(err)
        }

        f, err := os.Create(file.Name)
        if err != nil {
            fmt.Println(err)
        }
        defer f.Close()
        n, err := io.Copy(f, rc)
        if err != nil {
            fmt.Println(err)
        }
        fmt.Println(n)
		break
    }

}