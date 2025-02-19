// netbase project main.go
package netbase

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
)

func GetLocalAddr() string {
	conn, err := net.Dial("tcp", "www.baidu.com:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return strings.Split(conn.LocalAddr().String(), ":")[0]
}

//返回mac地址数组
func GetAllMACAddress() []string {
	// 获取本机的MAC地址
	MACs := []string{}
	interfaces, err := net.Interfaces()
	if err != nil {
		panic("Error : " + err.Error())
	}
	for _, inter := range interfaces {
		m := inter.HardwareAddr //获取本机MAC地址
		//fmt.Println("MAC = ", m)
		m1 := strings.Replace(fmt.Sprintf("%v", m), ":", "-", -1)
		MACs = append(MACs, m1)
	}
	return MACs
}

/** 通过调用接口获取外网IP
 */
func GetExternalIP() string {
	resp, err := http.Get("http://myexternalip.com/raw")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	content, _ := ioutil.ReadAll(resp.Body)
	//buf := new(bytes.Buffer)
	//buf.ReadFrom(resp.Body)
	//s := buf.String()
	return string(content)
}

/** 通过DNS获取万网IP
 */
func GetPulicIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	localAddr := conn.LocalAddr().String()
	idx := strings.LastIndex(localAddr, ":")
	return localAddr[0:idx]
}

/*
func main() {
	fmt.Println("Hello World!")
}
*/
