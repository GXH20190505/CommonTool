package timebase

import (
	"testing"
	"time"
)

func TestFormatDay(t *testing.T) {
	resStr:=FormatDay(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转整日",resStr)
}

func TestFormatDayEnd(t *testing.T) {
	resStr:=FormatDayEnd(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转整日",resStr)
}

func TestFormatHour(t *testing.T) {
	resStr:=FormatHour(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转小时",resStr)
}

func TestFormatHourEnd(t *testing.T) {
	resStr:=FormatHourEnd(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转小时",resStr)
}

func TestFormatMinute(t *testing.T) {
	resStr:=FormatMinute(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转分钟",resStr)
}

func TestFormatMinute10(t *testing.T) {
	resStr:=FormatMinute10(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转分钟10",resStr)
}

func TestFormatMinute30(t *testing.T) {
	resStr:=FormatMinute30(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转分钟30",resStr)
}

func TestFormatMinuteEnd10(t *testing.T) {
	resStr:=FormatMinuteEnd10(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转分钟end10",resStr)
}

func TestFormatMonth(t *testing.T) {
	resStr:=FormatMonth(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转整月",resStr)
}

func TestFormatMonthDayCount(t *testing.T) {
}

func TestFormatMonthDays(t *testing.T) {

}

func TestFormatMonthEnd(t *testing.T) {
	resStr:=FormatMonthEnd(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转整月",resStr)
}

func TestFormatYYYYMM(t *testing.T) {
	resStr:=FormatYYYYMM("2021-09-24 13:47:58")//time.Now(),20060102150405
	t.Log("时间转年月",resStr)
}

func TestFormatYear(t *testing.T) {
	resStr:=FormatYear(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转整年",resStr)
}

func TestFormatYearEnd(t *testing.T) {
	resStr:=FormatYearEnd(time.Now())//2021-09-24 13:47:58,20060102150405
	t.Log("时间转整年",resStr)
}

func TestGetInterval(t *testing.T) {
	resFloat,err:=GetInterval("2021-09-24 13:47:58","2021-09-24 13:57:02","MINUTE")
	if err!=nil{
		t.Log("时间间隔错误",err)
	}else {
		t.Log("时间间隔为",resFloat)
	}
}

func TestGetIntervalLocal(t *testing.T) {
	resFloat,err:=GetIntervalLocal("2021-09-24 13:47:58",time.Now(),"MINUTE")
	if err!=nil{
		t.Log("接口方法时间间隔错误",err)
	}else {
		t.Log("接口方法时间间隔为",resFloat)
	}
}

func TestGetMonday(t *testing.T) {
	resTime:=GetMonday(time.Now())
	t.Log("上周一日期",resTime)
}

func TestIsNowDay(t *testing.T) {
	resBol:=IsNowDay(time.Now().AddDate(0,0,-1))
	t.Log("是否为当日",resBol)
}

func TestNewTime(t *testing.T) {
  timeInfo:=NewTime(time.Now())
  t.Logf("%+v",timeInfo)
}

func TestNowTime(t *testing.T) {
	resStr:=NowTime()
	t.Log("当前时间为",resStr)
}

func TestNowTimeFormat(t *testing.T) {
	resStr:=NowTimeFormat()
	t.Log("当前时间为",resStr)
}

func TestParse(t *testing.T) {

}

func TestParseInLocation(t *testing.T) {

}

func TestTimeFormat(t *testing.T) {
	resStr:=TimeFormat("20210924133745")
	t.Log("字符串转日期",resStr)
}

func TestTimeFormatLocal(t *testing.T) {
	resStr:=TimeFormatLocal(time.Now())//20210924133745
	t.Log("字符串转日期接口",resStr)
}

func TestTimeFormatNon(t *testing.T) {
	resStr:=TimeFormatNon("2006-01-02 15:04:05")
	t.Log("字符串转日期",resStr)
}

func TestTimeInfo_GetDayMinute(t *testing.T) {
	timeInfo:=NewTime(time.Now())
	resInt:=timeInfo.GetDayMinute("2021-09-24 09:00:00","2021-09-24 12:00:00")
	t.Log("时间段分钟数",resInt)
}

func TestTimeNonToSta(t *testing.T) {
	resStr:=TimeNonToSta("20060102150405")
	t.Log("字符串转日期",resStr)
}

func TestTimeScope(t *testing.T) {
	resBol:=TimeScope("2021-09-23 13:47:58","2021-09-25 13:47:58","2021-09-24 13:47:58")
	t.Log("是否在范围内",resBol)
}

func TestTimeToTimestamp(t *testing.T) {
	resInt64:=TimeToTimestamp(time.Now())
	t.Log("日期转时间",resInt64)
}

func TestTimeoutAdjustMinute(t *testing.T) {
	resBol:=TimeoutAdjustMinute("2021-09-24 13:47:58",5)
	t.Log("日期是否超时",resBol)
}

func TestWeekByDate(t *testing.T) {
	resInt:=WeekByDate(time.Now())
	t.Log("本年是第几周",resInt)
}
