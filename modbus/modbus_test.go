package modbus

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func TestByteToFloat32(t *testing.T) {
  byts:=float32tobyte(3.14)
   flData:=ByteToFloat32(byts)
   t.Log(flData)
}
func float32tobyte(fl float32)[]byte{
	bits:=math.Float32bits(fl)
	bytes:=make([]byte,4)
	binary.LittleEndian.PutUint32(bytes,bits)
	return bytes
}
func TestBytesToInt(t *testing.T) {
	resultUInt162byte:=UInt16ToBytes(1024)
	resultInt:=BytesToInt(resultUInt162byte)
	t.Log("bytetoint", resultInt)
}
//大端在前
func TestBytesToString(t *testing.T) {
	resultInt32string2Byte:=Int32StringToBytes("1024")
	resultString:=BytesToString(resultInt32string2Byte)
	fmt.Println("bytetostring  ",resultString)
}

func TestBytesToString_L(t *testing.T) {
	resultInt322byte:=Int32ToBytes(1024)
	resultString:=BytesToString_L(resultInt322byte)
	fmt.Println("bytetostring_L ",resultString)
}

func TestCheckCrc(t *testing.T) {
	byteData:=make([]byte,8)
	byteData[0]=0x01
	byteData[1]=0x06
	byteData[2]=0x00
	byteData[3]=0x0A
	byteData[4]=0x01
	byteData[5]=0x10
	byteData[6]=0xA9
	byteData[7]=0x94
	crcData:= CheckSum(byteData)
	t.Log(crcData)
}

func TestCheckSum(t *testing.T) {
 byteData:=make([]byte,8)
 byteData[0]=0x01
byteData[1]=0x06
byteData[2]=0x00
byteData[3]=0x0A
byteData[4]=0x01
byteData[5]=0x10
byteData[6]=0xA9
byteData[7]=0x94
 resBool:= CheckCrc(byteData,6)
 if !resBool{
 	t.Log("crc校验失败")
 }
 t.Log("CRC校验成功")
}

func TestInt16StringToBytes(t *testing.T) {
	resByte := Int16StringToBytes("1024")
	t.Log(resByte)
}
/** 整形转换成字节数组
返回2字节数组
注需要反序
如 1→ []byte{0x01,0x00}
注：转换时，数据类型一定要指明是int16\int8\int32等，否则无法转换
*/
func TestInt16ToBytes(t *testing.T) {

	resultInt162byte:=Int16ToBytes(1024)
	t.Log("int16tobytes  ",resultInt162byte)
}
/** 整形转4字数组
1024→ []byte{0x00,0x04，0x00，0x00} */
func TestInt32ToBytes(t *testing.T) {
	resultInt322byte:=Int32ToBytes(1024)
	fmt.Println("int32tobytes  ",resultInt322byte)
}

/** 字符型数字，转字节数组
  1 → []byte{0x00,0x00,0x00,0x01}
*/
func TestInt32StringToBytes(t *testing.T) {
	resultInt32string2Byte:=Int32StringToBytes("1024")
	t.Log("TestInt32StringToBytes",resultInt32string2Byte)
}

func TestString2Bytes(t *testing.T) {
	resultByte :=String2Bytes("40 00")
	t.Log("string2byte  ",resultByte)
}
/** 无单位整形转数组
 */
func TestUInt16ToBytes(t *testing.T) {
	resultUInt162byte:=UInt16ToBytes(1024)
	t.Log("UInt16ToBytes  ",resultUInt162byte)
}
