package modbus

/** 十六进制校验
datalen 表示data的数据区，不能包括校验位置
*/
func CheckCrc(data []byte, datalen int) bool {
	if len(data) < datalen-2 {
		return false
	}
	crc := CheckSum(data[:datalen])
	if crc[0] != data[datalen] || crc[1] != data[datalen+1] {
		return false
	}
	return true
}
