package P2PClient

import (
	"bytes"
	"errors"
	"log"
	"net"
	"os/exec"
	"strings"

	"github.com/codeskyblue/go-sh"
)

// <summary>
// 启动程序
// </summary>
// <param name="serviceName">程序的名称</param>
func (sys *LinuxSys) ProcessStart(serviceName string) error {

	s := sh.NewSession()
	s.ShowCMD = true
	err := s.Command("sh", "-c", "service "+serviceName+" start").Run()
	if err != nil {
		log.Printf("Error %v starting process!", err)
		return err
	}
	//判断是否启动成功,通过pid是否有返回值
	cmd := s.Command("sh", "-c", "service "+serviceName+" stat")
	var buff bytes.Buffer
	cmd.Stdout = &buff
	err = cmd.Run()
	if err != nil {
		log.Printf("Error %v read process pid!", err)
		return err
	}

	line, errs := buff.ReadString('\n')
	if errs != nil {
		return err
	} else {
		if strings.TrimSpace(line) != "" {
			log.Printf("%v process pid:%v", serviceName, line)
			return nil
		} else {
			errors.New("fault")
		}
	}
	return nil

}

// <summary>
// 停止程序
// </summary>
// <param name="serviceName">程序的serviceName</param>
func (sys *LinuxSys) ProcessStop(serviceName string) error {

	s := sh.NewSession()
	s.ShowCMD = true
	err := s.Command("sh", "-c", " service "+serviceName+" stop").Run()
	if err != nil {
		log.Printf("Error %v starting process!", err)
		return err
	}
	//判断是否停止成功,通过pid是否有返回值
	cmd := s.Command("sh", "-c", "service "+serviceName+" stat")
	var buff bytes.Buffer
	cmd.Stdout = &buff
	err = cmd.Run()
	if err != nil {
		log.Printf("Error %v read process pid!", err)
		return err
	}

	line, errs := buff.ReadString('\n')
	if errs != nil {
		return err
	} else {
		if strings.TrimSpace(line) != "" {
			errors.New("fault")
		} else {
			return nil
		}
	}
	return nil
}

// <summary>
// 重启程序
// </summary>
// <param name="serviceName">程序的serviceName</param>
func (sys *LinuxSys) ProcessRestart(serviceName string) error {

	s := sh.NewSession()
	s.ShowCMD = true
	err := s.Command("sh", "-c", " service "+serviceName+" restart").Run()
	if err != nil {
		log.Printf("Error %v starting process!", err)
		return err
	}
	//判断是否启动成功,通过pid是否有返回值
	cmd := s.Command("sh", "-c", "service "+serviceName+" stat")
	var buff bytes.Buffer
	cmd.Stdout = &buff
	err = cmd.Run()
	if err != nil {
		log.Printf("Error %v read process pid!", err)
		return err
	}

	line, errs := buff.ReadString('\n')
	if errs != nil {
		return err
	} else {
		if strings.TrimSpace(line) != "" {
			log.Printf("%v process pid:%v", serviceName, line)
			return nil
		} else {
			errors.New("fault")
		}
	}
	return nil
}

// <summary>
// 读取进程信息
// </summary>
// <param name="processName">进程名称</param>
func GetProcessInfo(processName string) []string {
	var proInfo = make([]string, 0)

	//根据进程名称读取PID，cpu使用率，内存使用率，内存使用大小，进程路径
	cmd := exec.Command("/bin/sh", "-c", `ps -eo pid,%cpu,%mem,rss,command | grep `+processName+`| grep -v grep`)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err == nil {
		for {
			line, err := out.ReadString('\n')
			if err != nil {
				break
			}
			//判断读取的信息中是否包含进程名
			if ok := strings.Contains(line, "/"+processName); ok {
				proInfo = append(proInfo, line)
			}
		}
		//对字符串进行拆分
		if len(proInfo) > 0 {
			ft := make([]string, 0)
			for _, st := range proInfo {
				var strs string
				tokens := strings.Split(strings.Replace(st, "\n", "", 0), " ")
				for _, t := range tokens {
					if t != "" && t != "\t" && t != "\n" {
						strs += t + ";"
					}
				}
				ft = append(ft, strs)
			}

			return ft
		}
	}
	return make([]string, 0)
}

// <summary>
// IP信息
// </summary>
func NetInfo() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", errors.New("no find ip")
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("no find ip")
}

// <summary>
// 读取内存信息
// </summary>
func LinuxMemInfo() {

	//	a, _ := mem.SwapMemory()

	//	//内存使用情况
	//	b, _ := mem.VirtualMemory()

}

// <summary>
// 读取磁盘信息
// </summary>
func LinuxDiskInfo(path string) {
	//	//磁盘操作信息
	//	a, _ := disk.DiskIOCounters()

	//	//磁盘类型
	//	b, _ := disk.DiskPartitions(true)

	//	//磁盘空间使用信息
	//	c, _ := disk.DiskUsage(path)

}

// <summary>
// 读取CPU信息
// </summary>
func LinuxCpuInfo() {
	//	//cpu核数
	//	a, _ := cpu.CPUCounts(true)

	//	//cpu基本信息
	//	b, _ := cpu.CPUInfo()

	//	//CPU运行状态
	//	c, _ := cpu.CPUTimes(true)

}
