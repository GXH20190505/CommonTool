//时间相关操作
package timebase

import (
	"fmt"
	_ "fmt"
	"strconv"
	"strings"
	"time"
)

/** 时间定义
 */
type TimeInfo struct {
	Time          time.Time
	HourStart     string
	HourEnd       string
	DayStart      string //当日开始时间 如：2020-01-01 00:00:00
	DayEnd        string //当日结束时间 如：2020-01-01 23:59:59
	MonthDays     int    // 这个月有多少天
	MonthDayCount int    //  当月第几天
	WeekStart     string //周一开始时间
	WeekEnd       string //周一结束时间
	WeekIndex     string //本年第几周
	MonthStart    string //月开始时间
	MonthEnd      string //月结束时间
	SeasonStart   string //季开始时间
	SeasonEnd     string //季结束时间
	YearStart     string
	YearEnd       string
	FormatYYYYMM  string //按YYYYMM格式存储时间

	DayString   string //格式-- yyyy-MM-dd
	MonthString string //格式 -- yyyy-MM
	YearString  string // 格式 -- yyyy
}

func NewTime(v interface{}) *TimeInfo {
	tInfo := new(TimeInfo)
	switch v.(type) {
	case string:
		tInfo.Time = Parse(v.(string))
	case time.Time:
		tInfo.Time = v.(time.Time)
	default:
		return nil
	}
	tInfo.init()
	return tInfo
}

func (this *TimeInfo) init() {

	this.HourStart = FormatHour(this.Time)
	this.HourEnd = FormatHourEnd(this.Time)

	this.DayStart = FormatDay(this.Time)
	this.DayEnd = FormatDayEnd(this.Time)

	monTime := GetMonday(this.Time)
	this.WeekStart = FormatDay(monTime)
	this.WeekEnd = FormatDayEnd(monTime.AddDate(0, 0, 6))

	this.MonthStart = FormatMonth(this.Time)
	this.MonthEnd = FormatMonthEnd(this.Time)
	this.MonthDays = FormatMonthDays(this.MonthEnd)
	this.YearStart = FormatYear(this.Time)
	this.YearEnd = FormatYearEnd(this.Time)

	this.FormatYYYYMM = FormatYYYYMM(this.Time)
	this.WeekIndex = fmt.Sprintf("%d", WeekByDate(this.Time))
	
	this.SeasonStart = FormatSeason(this.Time)
	this.SeasonEnd = FormatSeasonEnd(this.Time)

	this.DayString = this.DayStart[:10]
	this.MonthString = this.DayStart[:7]
	this.YearString = this.DayStart[:4]
	this.MonthDayCount = FormatMonthDayCount(this.DayString)
}

/** 时间格式转换为季度
 */
func FormatSeason(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	season := 1
	switch t.Month() {
	case 1, 2, 3:
		season = 1
	case 4, 5, 6:
		season = 4
	case 7, 8, 9:
		season = 7
	case 10, 11, 12:
		season = 10

	}
	return fmt.Sprintf("%d-%02d-01 00:00:00", t.Year(), season)
}
func FormatSeasonEnd(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}

	switch t.Month() {
	case 1, 2, 3:
		return fmt.Sprintf("%d-03-31 23:59:59", t.Year())
	case 4, 5, 6:
		return fmt.Sprintf("%d-06-30 23:59:59", t.Year())
	case 7, 8, 9:
		return fmt.Sprintf("%d-09-30 23:59:59", t.Year())
	case 10, 11, 12:
		return fmt.Sprintf("%d-12-31 23:59:59", t.Year())

	}
	return ""
}

//  时间字符串检查和修正 14位  长度不够 补充0   超过长度 截断
func FixTimeStr(dt string) string {
	if len(dt) >= 14 {
		return dt[:14]
	} else {
		return fmt.Sprintf("%s%s", dt, fmt.Sprintf("%0*d", 14-len(dt), 0))
	}
}
/** 获取时间段在当前日期的分钟数
注：开始时间，结束时间可能大于当日
*/
func (this *TimeInfo) GetDayMinute(strST, strED string) int {
	st := Parse(strST)
	if IsNowDay(st) == false {
		//特殊处理，如果开始时间的日期大于当前日期，则返回0
		if st.Day() > this.Time.Day() {
			return 0
		}
		strST = this.DayStart
	}
	ed := Parse(strED)
	if IsNowDay(ed) == false {
		//特殊处理，如果结束时间的日期，小于当前日期，则返回0
		if ed.Day() < this.Time.Day() {
			return 0
		}
		strED = this.DayEnd
	}

	s, _ := GetInterval(strST, strED, "MINUTE")
	if s < 0 {
		s = 0
	}
	return int(s)
}

const TIME_STA = "2006-01-02 15:04:05"
const TIME_NON = "20060102150405"
const TimeMilliSecond = "20060102150405.000"
const TIME_STA1 = "2006-01-02 15:04:05.000"
func NowTimeFormatMillisecond() string {
	return time.Now().Format(TIME_STA1)
}
func NowTimeFormat() string {
	return time.Now().Format(TIME_STA)
}

func NowTime() string {
	return time.Now().Format(TIME_NON)
}

func TimeFormat(stTime string) string {
	strFormat := TIME_NON
	if strings.Index(stTime, "-") > 0 {
		return stTime
	}
	st, err := time.Parse(strFormat, stTime)
	if err != nil {
		return ""
	}
	return st.Format(TIME_STA)
}

func TimeFormatLocal(st interface{}) string {
	switch st.(type) {
	case time.Time:
		t := st.(time.Time)
		return t.Local().Format(TIME_STA)
	case string:
		t := Parse(st.(string))
		return t.Local().Format(TIME_STA)
	}
	return ""
}

func TimeFormatNon(stTime string) string {
	strFormat := TIME_NON
	if strings.Index(stTime, "-") > 0 {
		strFormat = TIME_STA
	}
	st, err := time.Parse(strFormat, stTime)
	if err != nil {
		return ""
	}
	return st.Format(TIME_NON)
}

func TimeNonToSta(nonTime string) string {
	times, err := time.Parse(TIME_NON, nonTime)
	if err != nil {
		return nonTime
	}
	return times.Format(TIME_STA)
}

//判断时间超时
//sDT 判断时间，格式：yyyy-MM-dd HH:mm:ss
//intval 判断间隔，单位分钟
//返回true=不超时 返回false=超时
func TimeoutAdjustMinute(sDT string, intval int) bool {
	//时间格式转化
	strFormat := TIME_NON
	if strings.Index(sDT, "-") > 0 {
		strFormat = TIME_STA
	}
	nowTime := time.Now().Format(strFormat)
	v, _ := GetInterval(sDT, nowTime, "MINUTE")
	if v <= float64(intval) {
		return true
	}
	return false
}

/** 判断2个时间间隔
判断逻辑
edTime-stTime
unit定义：小时=HOUR 分=MINUTE 秒=SECOND,不是上述三种则按默认为秒返回
*/
func GetInterval(stTime string, edTime string, unit string) (float64, error) {
	st := Parse(stTime)
	ed := Parse(edTime)

	s := ed.Sub(st)
	switch strings.ToUpper(unit) {
	case "HOUR":
		return s.Hours(), nil
	case "MINUTE":
		return s.Minutes(), nil
	case "SECOND":
		return s.Seconds(), nil
	}
	return s.Seconds(), nil
}
func GetIntervalLocal(stTime interface{}, edTime interface{}, unit string) (float64, error) {
	st := ParseInLocation(stTime)
	ed := ParseInLocation(edTime)

	s := ed.Sub(st)
	switch strings.ToUpper(unit) {
	case "HOUR":
		return s.Hours(), nil
	case "MINUTE":
		return s.Minutes(), nil
	case "SECOND":
		return s.Seconds(), nil
	}
	return s.Seconds(), nil
}
func Parse(stTime string) time.Time {
	strFormat := TIME_NON
	if strings.Index(stTime, "-") > 0 {
		strFormat = TIME_STA
	}
	st, _ := time.Parse(strFormat, stTime)
	return st
}

/** 转换成UTC格式时间，比当前时间早8小时
 */
func ParseInLocation(st interface{}) time.Time {
	switch st.(type) {
	case time.Time:
		t := st.(time.Time)
		return t.Local()
	case string:
		stTime := st.(string)
		strFormat := TIME_NON
		if strings.Index(stTime, "-") > 0 {
			strFormat = TIME_STA
		}
		loc := time.Local
		st, _ := time.ParseInLocation(strFormat, stTime, loc)
		return st
	}
	return time.Now()
}

/** 时间格式化为年月
 */
func FormatYYYYMM(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("200601")
}

/** 时间格式化成分钟
 */
func FormatMinute(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01-02 15:04") + ":00"
}
func FormatMinuteEnd(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01-02 15:04") + ":59"
}

/** 时间格式化为整10分钟
00~09分钟转为00分钟，10~19分钟转为10分钟 。。。
*/
func FormatMinute10(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	sMinute := t.Minute() % 10 //多余的分钟数
	return t.Add(-time.Duration(sMinute)*time.Minute).Format("2006-01-02 15:04") + ":00"
}

func FormatMinuteEnd10(v interface{}) string {
	strs := FormatMinute10(v)
	return strs[:15] + "9:59"
}

/** 时间格式化为整30分钟
00~29分钟转为00分钟，30~59分钟转为30分钟
*/
func FormatMinute30(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	sMinute := t.Minute() % 30 //多余的分钟数
	return t.Add(-time.Duration(sMinute)*time.Minute).Format("2006-01-02 15:04") + ":00"
}

/** 时间格式化成小时
 */
func FormatHour(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01-02 15") + ":00:00"
}
func FormatHourEnd(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01-02 15") + ":59:59"
}

/** 时间格式转化为整日
 */
func FormatDay(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01-02 ") + "00:00:00"
}

func FormatDayEnd(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01-02 ") + "23:59:59"
}
func TimeStaToNon(nonTime string) string {
	times, err := time.Parse(TIME_STA, nonTime)
	if err != nil {
		fmt.Println(err)
		return nonTime
	}
	return times.Format(TIME_NON)
}

/** 时间格式转化为整月
 */
func FormatMonth(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return t.Format("2006-01") + "-01 00:00:00"
}

func FormatMonthDays(monthEnd string) int {
	t := Parse(monthEnd)
	return t.Day()
}

func FormatMonthDayCount(dayString string) int {
	count, _ := strconv.Atoi(dayString[len(dayString)-2:])
	return count
}

/** 时间格式转化为整月
 */
func FormatMonthEnd(v interface{}) string {
	strST := FormatMonth(v)
	t := Parse(strST)
	t = t.AddDate(0, 1, 0).Add(-1 * time.Second)
	return t.Format(TIME_STA)
}

/** 时间格式转化为整年
 */
func FormatYear(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return fmt.Sprintf("%d-01-01 00:00:00", t.Year())
}

/** 时间格式转化为整年
 */
func FormatYearEnd(v interface{}) string {
	var t time.Time
	switch v.(type) {
	case string:
		t = Parse(v.(string))
	case time.Time:
		t = v.(time.Time)
	default:
		return ""
	}
	return fmt.Sprintf("%d-12-31 23:59:59", t.Year())
}

/** 获取上周一的日期
 */
func GetMonday(nowTime time.Time) (MonTime time.Time) {
	switch nowTime.Weekday() {
	case time.Monday:
	case time.Tuesday:
		nowTime = nowTime.AddDate(0, 0, -1)
	case time.Wednesday:
		nowTime = nowTime.AddDate(0, 0, -2)
	case time.Thursday:
		nowTime = nowTime.AddDate(0, 0, -3)
	case time.Friday:
		nowTime = nowTime.AddDate(0, 0, -4)
	case time.Saturday:
		nowTime = nowTime.AddDate(0, 0, -5)
	case time.Sunday:
		nowTime = nowTime.AddDate(0, 0, -6)
	}
	MonTime = nowTime
	return
}

/** 本年第几周
 */
func WeekByDate(t time.Time) int {
	yearDay := t.YearDay()
	yearFirstDay := t.AddDate(0, 0, -yearDay+1)
	firstDayInWeek := int(yearFirstDay.Weekday())
	//今年第一周有几天
	firstWeekDays := 1
	if firstDayInWeek != 0 {
		firstWeekDays = 7 - firstDayInWeek + 2
	}

	var week int
	if yearDay <= firstWeekDays {
		week = 1
	} else {
		week = (yearDay-firstWeekDays)/7 + 2
	}

	return week
}

/** 是否为当日
 */
func IsNowDay(t time.Time) bool {
	st := time.Now()
	if t.Year() == st.Year() && t.Month() == st.Month() && t.Day() == st.Day() {
		return true
	}
	return false
}

/** 时间是否在范围内
 */
func TimeScope(sTime, eTime, dataTime string) bool {
	valTime := Parse(dataTime)
	startTime, sErr := time.Parse(TIME_STA, sTime)
	endTime, eErr := time.Parse(TIME_STA, eTime)
	if sErr == nil && eErr == nil {
		if valTime.Before(endTime) && startTime.Before(valTime) {
			return true
		}
	} else {
		fmt.Println(sErr)
		fmt.Println(eErr)
	}
	return false

}

func TimeToTimestamp(t time.Time) int64 {
	return t.UnixNano() * int64(time.Nanosecond) / int64(time.Millisecond)
}
