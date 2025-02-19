package compressbyshell

type Compress interface {
	/** 初始化 --
		windows系统需要检查好压是否正常安装配置
		Linux系统检查命令是否能正常执行
	*/
	Init() error
	
	/** zip包压缩
		windows系统调用好压进行压缩
		Linux系统调用系统指令压缩
		zipAddr := 压缩后zip文件的地址 如: "D:/1234.zip" or "/usr/bin/1234.zip"
		fileAddr := 需要压缩的文件     如: "D:/1234.txt" or "/usr/bin/1234.txt"
	*/
	CompressZip(zipAddr string, fileAddr string) error
	
	/** zip包压缩--多文件压缩
		注：此处每个文件都需要绝对路径，否则会找不到文件导致压缩失败
	*/
	CompressZipAll(zipAddr string,fileAddrs []string) error
	
	/** zip包解压缩
		从指定的压缩包中解压出指定的文件
		如果FileAddr为文件夹名，需要在最后加"/"
	*/
	UncompressZip(zipAddr string,fileAddr string) error
	
	/** tar.gz格式压缩
	*/
	CompressTarGzAll(tarAddr string, FileAddr []string) error
	
	/** tar.gz格式解压缩
		tarAddr  == 需要解压缩的tar.gz文件地址，绝对路径，包括扩展名
		FileAddr == 解压的文件名，如果为空，则默认解压到tar.gz文件目录下
		注：如果FileAddr为文件夹名，需要在最后加"/"
	*/
	UncompressTarGz(tarAddr string,fileAddr string) error
	
	/** tar.bz2格式压缩
	*/
	CompressTarBzip2(tarAddr string,fileAddrs string) error
	
	/** tar.bz2格式压缩--多文件
	*/
	CompressTarBzip2All(tarAddr string,fileAddrs []string) error
	
	
}

