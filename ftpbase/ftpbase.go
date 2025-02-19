package ftpbase

import (
	"github.com/jlaffaye/ftp"
	"os"
)

// 连接到FTP服务器
func connectFTP(addr, user, password string) (*ftp.ServerConn, error) {
	conn, err := ftp.Dial(addr)
	if err!= nil {
		return nil, err
	}

	err = conn.Login(user, password)
	if err!= nil {
		conn.Quit()
		return nil, err
	}

	return conn, nil
}

// 上传文件到FTP服务器
func uploadFile(conn *ftp.ServerConn, localFilePath, remoteFilePath string) error {
	localFile, err := os.Open(localFilePath)
	if err!= nil {
		return err
	}
	defer localFile.Close()

	err = conn.Stor(remoteFilePath, localFile)
	if err!= nil {
		return err
	}

	return nil
}

// 从FTP服务器下载文件
func downloadFile(conn *ftp.ServerConn, remoteFilePath, localFilePath string) error {
	remoteFile, err := conn.Retr(remoteFilePath)
	if err!= nil {
		return err
	}
	defer remoteFile.Close()

	localFile, err := os.Create(localFilePath)
	if err!= nil {
		return err
	}
	defer localFile.Close()

	_, err = io.Copy(localFile, remoteFile)
	if err!= nil {
		return err
	}

	return nil
}