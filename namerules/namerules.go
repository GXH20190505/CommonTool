package namerules

import (
	"strings"
	"time"
	"fmt"
)


const DATATYPE_REAL 	= "REAL"
const DATATYPE_MINUTE 	= "MINUTE"
const DATATYPE_HOUR 	= "HOUR"
const DATATYPE_DAY 		= "DAY"

const CN_REAL			= "2011"
const CN_MINUTE			= "2051"
const CN_HOUR			= "2061"
const CN_DAY 			= "2031"


/** 212协议表名
	参数定义：
	MCUSN : 设备编码 212写的MN号
	CN    : 协议类型 2011=实时数据 2051=分钟数据 2061=小时数据 2031=日数据
	
	返回值：
	RawName : 原始数据表名    T_RAW_REAL_8888000001
	RmcName : 处理后的数据表名 T_RMC_REAL_8888000001
	
*/
func TableName_HJ212(MCUSN string,CN string) (RawName string){
	var dtType string
	
	switch CN {
		case CN_REAL 	: dtType = DATATYPE_REAL
		case CN_MINUTE 	: dtType = DATATYPE_MINUTE
		case CN_HOUR 	: dtType = DATATYPE_HOUR
		case CN_DAY 	: dtType = DATATYPE_DAY
		default			: return 
	}
	return strings.ToUpper(fmt.Sprintf("T_%s_%s",dtType,MCUSN))
}

/** 212协议表名
	说明：
		按月分表
	参数定义：
		MCUSN : 设备编码 212写的MN号
		CN    : 协议类型 2011=实时数据 2051=分钟数据 2061=小时数据 2031=日数据
		STIME : 指定时间，如果为空，则默认采用当月为表名
	返回值：
		RawName : 原始数据表名    T_RAW_REAL_8888000001
		RmcName : 处理后的数据表名 T_RMC_REAL_8888000001
	
*/
func TableName_HJ212_Month(MCUSN string,CN string,sTime string) (RawName string, RmcName string){
	var dtType string
	if sTime==""{
		sTime= time.Now().Format("200601")
	}
	switch CN {
		case CN_REAL : dtType = DATATYPE_REAL
		case CN_MINUTE : dtType = DATATYPE_MINUTE
		case CN_HOUR : dtType = DATATYPE_HOUR
		case CN_DAY : dtType = DATATYPE_DAY
		default 	: return 
	}
	return fmt.Sprintf("T_RAW_%s_%s_%s",dtType,sTime,MCUSN),fmt.Sprintf("T_RMC_%s_%s_%s",dtType,sTime,MCUSN)
}

/** 212协议表名
	说明：
		按年分表
	参数定义：
		MCUSN : 设备编码 212写的MN号
		CN    : 协议类型 2011=实时数据 2051=分钟数据 2061=小时数据 2031=日数据
		STIME : 指定时间，如果为空，则默认采用当月为表名
	返回值：
		RawName : 原始数据表名    T_RAW_REAL_8888000001
		RmcName : 处理后的数据表名 T_RMC_REAL_8888000001
	
*/
func TableName_HJ212_Year(MCUSN string,CN string,sTime string) (RawName string, RmcName string){
	var dtType string
	if sTime==""{
		sTime= time.Now().Format("2006")
	}
	switch CN {
		case CN_REAL : dtType = DATATYPE_REAL
		case CN_MINUTE : dtType = DATATYPE_MINUTE
		case CN_HOUR : dtType = DATATYPE_HOUR
		case CN_DAY : dtType = DATATYPE_DAY
		default 	: return 
	}
	return fmt.Sprintf("T_RAW_%s_%s_%s",dtType,sTime,MCUSN),fmt.Sprintf("T_RMC_%s_%s_%s",dtType,sTime,MCUSN)
}

/** 212协议因子名称转字段名
	说明
	转换方法，根据因子类型转换
	如： S01-Rtd -> Rtd_S01   S01-Flag -> Flag_S01
		S01-Min -> Min_S01   S01-Max -> Max_S01
	如果转换不正确，则返回空字符串
*/
func FactorToColumnName(Factor string) (string) {
	if strings.Index(Factor,"-")<=0{
		return ""
	}
	strs := strings.Split(Factor,"-")
	return fmt.Sprintf("%s_%s",strs[1],strs[0])
}

/** 212协议因子名称转字段名
	说明
	转换方法，根据因子类型转换
	如： S01-Rtd -> Rtd_S01   S01-Flag -> Flag_S01
		S01-Min -> Min_S01   S01-Max -> Max_S01
	如果转换不正确，则返回空字符串
	返回值分别为：转换后的名称（列名）、因子编码、因子标识
*/
func FactorToSplit(Factor string) (string,string,string) {
	if strings.Index(Factor,"-")<=0{
		return "",Factor,""
	}
	strs := strings.Split(Factor,"-")
	return fmt.Sprintf("%s_%s",strs[1],strs[0]),strs[0],strs[1]
}