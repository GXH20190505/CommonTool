package namerules

import (
	"testing"
)

/** 212协议因子名称转字段名
说明
转换方法，根据因子类型转换
如： S01-Rtd -> Rtd_S01   S01-Flag -> Flag_S01
	S01-Min -> Min_S01   S01-Max -> Max_S01
如果转换不正确，则返回空字符串
abc-def -> def_abc

*/
func TestFactorToColumnName(t *testing.T) {
	resfcName:= FactorToColumnName("abc-def")
	t.Log(resfcName)
}
/** 212协议因子名称转字段名
说明
转换方法，根据因子类型转换
如： S01-Rtd -> Rtd_S01   S01-Flag -> Flag_S01
	S01-Min -> Min_S01   S01-Max -> Max_S01
如果转换不正确，则返回空字符串
返回值分别为：转换后的名称（列名）、因子编码、因子标识
传入abc-def
返回def_abc，abc，def
*/
func TestFactorToSplit(t *testing.T) {
	a,b,c:=FactorToSplit("abc-def")
	t.Log(a,b,c)
}
/** 212协议表名
参数定义：
MCUSN : 设备编码 212写的MN号
CN    : 协议类型 2011=实时数据 2051=分钟数据 2061=小时数据 2031=日数据

返回值：
RawName : 原始数据表名    T_RAW_REAL_8888000001
RmcName : 处理后的数据表名 T_RMC_REAL_8888000001
传入:abcdefg  返回:T_REAL_ABCDEFG
*/
func TestTableName_HJ212(t *testing.T) {
	resName:=TableName_HJ212("abcdefg","2011")
	t.Log(resName)
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

 传入 abcdefg,2011,202109
 返回：T_RAW_REAL_202109_abcdefg T_RMC_REAL_202109_abcdefg
*/
func TestTableName_HJ212_Month(t *testing.T) {
	resRaw,resRmc:=TableName_HJ212_Month("abcdefg","2011","202109")
	t.Log(resRaw,resRmc)

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
传入 abcdefg,2011,2021
返回 T_RAW_REAL_2021_abcdefg T_RMC_REAL_2021_abcdefg
*/
func TestTableName_HJ212_Year(t *testing.T) {
	resRaw,resRmc:=TableName_HJ212_Year("abcdefg","2011","2021")
	t.Log(resRaw,resRmc)
}
