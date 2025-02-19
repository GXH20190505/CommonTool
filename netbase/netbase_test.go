package netbase

import (
	"testing"
)
/*获取本地mac地址*/
func TestGetAllMACAddress(t *testing.T) {
	localMac:=GetAllMACAddress()
	t.Log(localMac)
}
//调用接口获取外网IP
func TestGetExternalIP(t *testing.T) {
	exterIP:=GetExternalIP()//会出现超时情况，无法获取外网IP
	t.Log(exterIP)
}
/*获取本地地址*/
func TestGetLocalAddr(t *testing.T) {
	localAddr :=GetLocalAddr()
	t.Log(localAddr)
}
/** 通过DNS获取万网IP
 */
func TestGetPulicIP(t *testing.T) {
	pubIP:=GetPulicIP()
	t.Log(pubIP)
}
