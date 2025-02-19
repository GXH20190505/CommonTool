package modbus

import (
	"fmt"
)

func ModbusTest() {
	/** 十六进制字符串转字节数组
	  处理带空格和不带空格两种情况

	*/
	resultByte :=String2Bytes("40 00")
	fmt.Println("string2byte  ",resultByte)

	/** 整形转换成字节数组
	返回2字节数组

	注需要反序
	如 1→ []byte{0x01,0x00}
	注：转换时，数据类型一定要指明是int16\int8\int32等，否则无法转换
	*/
	resultInt162byte:=Int16ToBytes(1024)
	fmt.Println("int16tobytes  ",resultInt162byte)


	/** 整形转4字数组
	1024→ []byte{0x00,0x04，0x00，0x00} */
	resultInt322byte:=Int32ToBytes(1024)
	fmt.Println("int32tobytes  ",resultInt322byte)

	/** 无单位整形转数组
	 */
	resultUInt162byte:=UInt16ToBytes(1024)
	fmt.Println("UInt16ToBytes  ",resultUInt162byte)

	/** 字符型数字，转字节数组
	  1 → []byte{0x00,0x00,0x00,0x01}
	*/
 resultInt32string2Byte:=	Int32StringToBytes("1024")

	Int16StringToBytes("1024")

	/** 字节转整形
	 */
	 resultInt:=BytesToInt(resultUInt162byte)
	fmt.Println("bytetoint", resultInt)

	 //大端在前
	resultString:= BytesToString(resultInt32string2Byte)
	fmt.Println("bytetostring  ",resultString)

	//小端在前
	resultString = BytesToString_L(resultInt322byte)
	fmt.Println("bytetostring_L ",resultString)
}
