package modbus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

/** 十六进制字符串转字节数组
处理带空格和不带空格两种情况

*/
func String2Bytes(data string) []byte {
	var b1 bytes.Buffer
	var sp []string
	if strings.Contains(data, " ") { //可能会出现单字节的情况
		sp = strings.Split(data, " ")
	} else {
		for i := 0; i < len(data)/2; i++ {
			sp = append(sp, data[i*2:i*2+2])
		}
	}
	for i := 0; i < len(sp); i++ {
		d, _ := strconv.ParseInt(sp[i], 16, 0)
		b1.WriteByte(byte(d))
	}
	return b1.Bytes()
	/*只处理不带空格的方案
	count := len(data) / 2
	for i := 0; i < count; i++ {
		d, _ := strconv.ParseInt(data[i*2:i*2+2], 16, 0)
		b1.WriteByte(byte(d))
	}
	return b1.Bytes()*/
}

/** 整形转换成字节数组
返回2字节数组

注需要反序
如 1→ []byte{0x01,0x00}
注：转换时，数据类型一定要指明是int16\int8\int32等，否则无法转换
*/
func Int16ToBytes(n int) []byte {
	x := int16(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.LittleEndian, x) //LittleEndian 表示低位在前  BigEndian表示高位在前
	return bytesBuffer.Bytes()
}

/** 整形转4字数组
 */
func Int32ToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.LittleEndian, x) //LittleEndian 表示低位在前  BigEndian表示高位在前
	return bytesBuffer.Bytes()
}

/** 无单位整形转数组
 */
func UInt16ToBytes(n uint16) []byte {
	var v1 bytes.Buffer
	binary.Write(&v1, binary.LittleEndian, n)
	return v1.Bytes()
}

/** 字符型数字，转字节数组
1 → []byte{0x00,0x00,0x00,0x01}
*/
func Int32StringToBytes(n string) []byte {
	x, _ := strconv.Atoi(n)
	var v1 bytes.Buffer
	binary.Write(&v1, binary.BigEndian, int32(x))
	fmt.Println(n, x, v1.Bytes())
	return v1.Bytes()
}
func Int16StringToBytes(n string) []byte {
	x, _ := strconv.Atoi(n)
	var v1 bytes.Buffer
	binary.Write(&v1, binary.BigEndian, int16(x))
	fmt.Println(n, x, v1.Bytes())
	return v1.Bytes()
}

/** 字节转整形
 */
func BytesToInt(b []byte) int {
	if len(b) == 2 {
		bytesBuffer := bytes.NewBuffer([]byte{})
		bytesBuffer.Write(b)
		var x int16
		binary.Read(bytesBuffer, binary.LittleEndian, &x)
		return int(x)
	}
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.LittleEndian, &x)
	return int(x)
}

/** 字节转int型字符串
 */
func BytesToString(b []byte) string {
	if len(b) == 2 {
		bytesBuffer := bytes.NewBuffer([]byte{})
		bytesBuffer.Write(b)
		var x int16
		binary.Read(bytesBuffer, binary.BigEndian, &x)
		return strconv.Itoa(int(x))
	}
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return strconv.Itoa(int(x))
}

func BytesToString_L(b []byte) string {
	if len(b) == 2 {
		bytesBuffer := bytes.NewBuffer([]byte{})
		bytesBuffer.Write(b)
		var x int16
		binary.Read(bytesBuffer, binary.LittleEndian, &x)
		return strconv.Itoa(int(x))
	}
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.LittleEndian, &x)
	return strconv.Itoa(int(x))
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)

	return math.Float32frombits(bits)
}
