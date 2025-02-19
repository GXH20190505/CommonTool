package P2PClient

import (
	"fmt"
	"log"

	"github.com/codeskyblue/go-sh"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	_ "github.com/shirou/gopsutil/docker"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"github.com/shirou/gopsutil/process"
)

// <summary>
// 启动程序
// </summary>
// <param name="serviceName">程序的名称</param>
func (sys *WindowsSys) ProcessStart(serviceName string) error {

	s := sh.NewSession()
	s.ShowCMD = true
	err := s.Command("sh", "-c", "net start  "+serviceName).Run()
	if err != nil {
		log.Printf("Error %v starting process!", err)
		return err
	}
	return nil

}

// <summary>
// 停止程序
// </summary>
// <param name="serviceName">程序的serviceName</param>
func (sys *WindowsSys) ProcessStop(serviceName string) error {

	s := sh.NewSession()
	s.ShowCMD = true
	err := s.Command("sh", "-c", " net stop "+serviceName).Run()
	if err != nil {
		log.Printf("Error %v starting process!", err)
		return err
	}

	return nil
}

// <summary>
// 重启程序
// </summary>
// <param name="serviceName">程序的serviceName</param>
func (sys *WindowsSys) ProcessRestart(serviceName string) error {

	s := sh.NewSession()
	s.ShowCMD = true
	err := s.Command("cmd", "/C", " nssm restart "+serviceName).Run()
	if err != nil {
		log.Printf("Error %v starting process!", err)
		return err
	}

	return nil
}

func OtherWindow() {
	a, _ := load.LoadAvg()
	fmt.Println(a)

}

//内存信息
func MemInfoWindow() {

	a, _ := mem.SwapMemory()
	fmt.Println(a)
	//内存使用情况
	b, _ := mem.VirtualMemory()
	fmt.Println(b)
}

//网络信息
func NetInfoWindow() {
	a, _ := net.NetFilterCounters()
	fmt.Println(a)
	//本地网络信息
	b, _ := net.NetInterfaces()
	fmt.Println(b)
	//网卡信息
	c, _ := net.NetIOCounters(true)
	fmt.Println(c)
	d, _ := net.NetIOCountersByFile(true, "360rp.exe")
	fmt.Println(d)
	//端口信息
	//e,_:=net.NetProtoCounters()
}

//进程信息
func ProcessInfoWindow(pid int32) {

	//根据名称读取进程信息

	//	//根据pid查找进程信息
	//	a, _ := process.GetWin32Proc(7920)
	//	fmt.Println(a)
	//启动新的进程
	b, _ := process.NewProcess(pid)
	fmt.Println(b)
	//判断进程是否存在
	c, _ := process.PidExists(pid)
	fmt.Println(c)
	//读取所有PID
	d, _ := process.Pids()
	fmt.Println(d)
}

//磁盘
func DiskInfoWindow() {
	//磁盘操作信息
	a, _ := disk.DiskIOCounters()
	fmt.Println(a)
	//磁盘类型
	b, _ := disk.DiskPartitions(true)
	fmt.Println(b)
	//磁盘空间使用信息
	c, _ := disk.DiskUsage("c:")
	fmt.Println(c)
}

//CPU
func CpuInfoWindow() {
	//cpu核数
	a, _ := cpu.CPUCounts(true)
	fmt.Println(a)
	//cpu基本信息
	b, _ := cpu.CPUInfo()
	fmt.Println(b)
	//CPU运行状态
	c, _ := cpu.CPUTimes(true)
	fmt.Println(c)
}
