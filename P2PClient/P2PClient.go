package P2PClient

/*****************
网络客户端操作
******************/

import (
	"bytes"
	"errorlog"
	"fmt"
	"net"
	"sync"
	"time"
)

// <summary>
// 客户端连接包含信息
// </summary>
type Client struct {
	Address     string   //IP地址和端口
	UserId      int32    //客户端id
	SecKey      string   //客户端标识
	logined     bool     //在线状态
	session     net.Conn //连接对象
	clientMutex sync.Mutex
}

// <summary>
//对客户端进行初始化
// </summary>
// <param name="addr">配置文件路径</param>
// <param name="user">客户端id</param>
// <param name="key">客户端唯一标识</param>
// <returns></returns>
func NewClient(addr string, user int32, key string) *Client {
	c := new(Client)
	c.Address = addr
	c.UserId = user
	c.SecKey = key
	c.logined = false
	return c
}

// <summary>
//判断客户端是否连接  true:连接 false:未连接
// </summary>
func (c *Client) IsLogin() bool {
	return c.logined
}

// <summary>
//判断客户端是否连接,未连接时关闭客户端
// </summary>
func (c *Client) Logout() {
	c.logined = false
	c.clientMutex.Lock()
	defer c.clientMutex.Unlock()
	if c.session != nil {
		errorlog.ErrorLogInfo("client", "客户端离线", fmt.Sprintf("客户端离线,离线时间:%v", time.Now().Format("2006-01-02 15:04:05")))
		err := c.session.Close()
		if err != nil {
			//panic(err)
		}
	}
}

// <summary>
//从网络中读取数据
// </summary>
func (c *Client) ReadPacket(conn net.Conn) []byte {
	result := bytes.NewBuffer(nil)
	buf := make([]byte, 4096)

	if c.logined == false {
		return result.Bytes()
	}
	conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	if length, err := conn.Read(buf); err == nil {
		if length > 0 {
			buf[length] = 0
			result := buf[0:length]
			errorlog.ErrorLogDebug("clientdata", "接收数据",fmt.Sprintf("[%s] %s",conn.LocalAddr().String(),result))
			return result
		}
	}
	return result.Bytes()
}

// <summary>
//对socket连接进行初始化
// </summary>
func (c *Client) Handshake() bool {
	_, err := net.ResolveTCPAddr("tcp4", c.Address)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout("tcp", c.Address, time.Second*10) //net.DialTCP("tcp", nil, tcpAddr)

	if err != nil {
		return false
	}
	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetNoDelay(true)
	c.session = conn
	c.logined = true
	return true
}

// <summary>
//发送数据到服务端
// </summary>
// <param name="text">发送的内容</param>
func (c *Client) SendMessage(text string) bool {
	if c.logined == false {
		return false
	}
	_, err := c.session.Write([]byte(text))
	//发送失败后把状态改为false
	if err != nil {
		errorlog.ErrorLogDebug("clientdata", "发送失败",fmt.Sprintf("[%s] %s",c.session.LocalAddr().String(),text))
		c.logined = false
	} else {
		errorlog.ErrorLogDebug("clientdata", "发送成功",fmt.Sprintf("[%s] %s",c.session.LocalAddr().String(),text))
	}

	return err == nil
}
