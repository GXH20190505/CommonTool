package stringbase

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func Int2string(baseInt int)string  {
	return strconv.Itoa(baseInt)
}
/*获取字符串
 */
func GetInsertStr(baseStr string, startStr string, endStr string) (backstr string) {
	//判断起始字符串在源字符串中的位置
	st := strings.Index(baseStr, startStr)
	if st >= 0 {
		st += len(startStr)
	} else {
		return "" //没有找到头，返回空字符串
	}
	//剔除起始字符串之前的部分
	strs := strings.Split(baseStr, startStr)[1]
	//剔除结束字符以后的部分
	backstr = strings.Split(strs, endStr)[0]

	return backstr
}

/*汉字截取已保证不乱码
 */
func SubstrByByte(str string, length int) string {
	bs := []byte(str)[:length]
	bl := 0
	for i := len(bs) - 1; i >= 0; i-- {
		switch {
		case bs[i] >= 0 && bs[i] <= 127:
			return string(bs[:i+1])
		case bs[i] >= 128 && bs[i] <= 191:
			bl++
		case bs[i] >= 192 && bs[i] <= 253:
			cl := 0
			switch {
			case bs[i]&252 == 252:
				cl = 6
			case bs[i]&248 == 248:
				cl = 5
			case bs[i]&240 == 240:
				cl = 4
			case bs[i]&224 == 224:
				cl = 3
			default:
				cl = 2
			}
			if bl+1 == cl {
				return string(bs[:i+cl])
			}
			return string(bs[:i])
		}
	}
	return ""
}

/*流量单位转换

 */
func UnitOfbits(data interface{}) string {

	var s float64 = Float64(data)
	var unit string = "B"

	for s >= 1000 && unit != "T" {
		switch unit {
		case "B":
			unit = "K"
		case "K":
			unit = "M"
		case "M":
			unit = "G"
		case "G":
			unit = "T"
		default:
			continue
		}
		s = s / 1024
	}
	return fmt.Sprintf("%.1f%s", s, unit)
}

/** 字节单位转换，T、G等单位转换成字节
 */
func UnitToBits(data string) float64 {
	sVal := ""
	sUnit := ""
	for i := 0; i < len(data); i++ {
		switch string(data[i]) {
		case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".", "-":
		default:
			sVal = data[:i]
			sUnit = data[i:]
			break
		}
	}
	val := Float64(sVal)
	switch strings.ToUpper(sUnit) {
	case "T", "TB":
		val = val * 1024 * 1024 * 1024 * 1024
	case "G", "GB":
		val = val * 1024 * 1024 * 1024
	case "M", "MB":
		val = val * 1024 * 1024
	case "K", "KB":
		val = val * 1024
	}
	return val
}

/** 获取数据单位
 */
func GetUnit(data string) string {
	unit := ""
	for i := 0; i < len(data); i++ {
		switch string(data[i]) {
		case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".", "-":
		default:
			unit = data[i:]
			break
		}
	}
	return unit
}

func Float64(reply interface{}) float64 {
	switch reply := reply.(type) {
	case float64:
		return reply
	case int:
		return float64(reply)
	case int8:
		return float64(reply)
	case int32:
		return float64(reply)
	case int64:
		return float64(reply)
	case uint64:
		return float64(reply)
	case string:
		f, _ := strconv.ParseFloat(reply, 64)
		return f
	}
	return 0
}

func InterfaceToString(value interface{}) string {
	var key string
	if value == nil {
		return key
	}
	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}
	return key
}

func Int64(reply interface{}) int64 {
	switch reply := reply.(type) {
	case float64:
		return int64(reply)
	case int:
		return int64(reply)
	case int8:
		return int64(reply)
	case int32:
		return int64(reply)
	case int64:
		return reply
	case uint64:
		return int64(reply)
	case string:
		f, _ := strconv.ParseInt(reply, 10, 64)
		return f
	}
	return 0
}

func Int(reply interface{}) int {
	return int(Float64(reply))
}

/** 数据小数点位数处理
保留制定位数的小数位
如果Data="" 或者 Point="" 退出处理
处理逻辑，在Data的小数位后面添加Point个数个0，然后进行截取
*/
func FormatDataPoint(Data string, Point string) string {
	if Data == "" || Point == "" {
		return Data
	}

	s, err := strconv.ParseFloat(Data, 10)
	if err != nil {
		return Data
	}
	p, err := strconv.Atoi(Point)
	if err != nil {
		return Data
	}

	return strconv.FormatFloat(s, 'f', p, 64)
}

/** 判断字符串是否只包含字母和数字
 */
func IsRealString(data string) bool {

	for _, s := range data {
		switch {
		case s >= 'A' && s <= 'Z':
		case s >= 'a' && s <= 'z':
		case s >= '0' && s <= '9':
		default:
			return false
		}
	}
	return true
}

/** 从一组数据中获取最大值
 */
func GetMaxValue(sList string) string {
	sp := strings.Split(sList, ",")
	var fMax float64
	var sMax string
	for _, s := range sp {
		if s != "" {
			a, err := strconv.ParseFloat(s, -1)
			if err != nil {
				continue
			}
			if sMax == "" {
				fMax = a
				sMax = s
			} else if a > fMax {
				fMax = a
				sMax = s
			}
		}
	}
	return sMax
}

/** 从一组数据中获取最小值
 */
func GetMinValue(sList string) string {
	sp := strings.Split(sList, ",")
	var fMin float64
	var sMin string
	for _, s := range sp {
		if s != "" {
			a, err := strconv.ParseFloat(s, -1)
			if err != nil {
				continue
			}
			if sMin == "" {
				fMin = a
				sMin = s
			} else if a < fMin {
				fMin = a
				sMin = s
			}
		}
	}
	return sMin
}

/** 从一组数据中获取平均值
 */
func GetAvgValue(sList string) string {
	sp := strings.Split(sList, ",")
	var fAvg float64
	var num float64
	for _, s := range sp {
		if s != "" {
			a, err := strconv.ParseFloat(s, -1)
			if err != nil {
				continue
			}
			fAvg += a
			num++
		}
	}
	if num <= 0 {
		return ""
	}
	fAvg = fAvg / num
	return fmt.Sprintf("%0.3f", fAvg)
}

/** 从一组数据中获取累计值
 */
func GetCouValue(sList string) string {
	sp := strings.Split(sList, ",")
	var fCou float64
	var num int
	for _, s := range sp {
		if s != "" {
			a, err := strconv.ParseFloat(s, -1)
			if err != nil {
				continue
			}
			fCou += a
			num++
		}
	}
	if num <= 0 {
		num = 1
	}
	return fmt.Sprintf("%0.3f", fCou)
}

/** 从一组数据中出现最多的字符
 */
func GetMaxString(sList string) string {
	sp := strings.Split(sList, ",")
	mp := make(map[string]int)
	for _, s := range sp {
		mp[s]++
	}
	name := ""
	count := 0
	for k, v := range mp {
		if v > count {
			count = v
			name = k
		}
	}
	return name
}

/** json转换成string
 */
func JsonToString(v interface{}) string {
	jsd, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(jsd)
}

/** json转换成map
 */
func JsonToMap(v interface{}) map[string]string {
	mp := make(map[string]string)
	jsd, err := json.Marshal(v)
	if err != nil {
		fmt.Println("stringbase.JsonToMap", "转换失败", err, v)
		return mp
	}
	err = json.Unmarshal(jsd, &mp)
	if err != nil {
		fmt.Println("stringbase.JsonToMap", "转换失败", err, v)
	}
	return mp
}
