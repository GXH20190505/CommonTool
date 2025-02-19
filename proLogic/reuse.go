//传输中文乱码问题 
func GbToUtf8(s []byte) ([]byte, error) {
	//reader := transform.NewReader(byte.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	reader := transform.NewReader(bytes.NewReader(s),simplifiedchinese.GBK.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	return d, nil
}

//将时间转换为数字便于比较
func TimeToInt(nowTime time.Time) int {
	return nowTime.Year()*10000 + int(nowTime.Month())*100 + nowTime.Day()
}

//获取传入的时间所在月份的第一天，即某月第一天的0点。如传入time.Now(), 返回当前月份的第一天0点时间。
func GetFirstDateOfMonth(d time.Time) time.Time {
	d = d.AddDate(0, 0, -d.Day() + 1)
	return GetZeroTime(d)
}

//获取某一天的0点时间
func GetZeroTime(d time.Time) time.Time {
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, d.Location())
}

/** 计算源偏移距离
 */
func EarthDistance(lat1, lng1, lat2, lng2 float64) float64 {
	radius := float64(6371000) // 6378137
	rad := math.Pi / 180.0

	lat1 = lat1 * rad
	lng1 = lng1 * rad
	lat2 = lat2 * rad
	lng2 = lng2 * rad

	theta := lng2 - lng1
	dist := math.Acos(math.Sin(lat1)*math.Sin(lat2) + math.Cos(lat1)*math.Cos(lat2)*math.Cos(theta))

	return dist * radius
}

/** 离线报警判断
 */
func (this *DeviceClient) analyzer_alarm_offline() {
	/*if this.IsAlarm_Fault == ALARM_ON {
		return
	}*/
	var alarmText string
	if this.LastTime == "" { //最后上报时间为空不做处理,可能是刚上线的设备
		this.IsAlarm_OffLine = ALARM_ON
		this.OnLineTime = ""
		errorlog.ErrorLogDebug("onlinetime", this.devCode, "lasttime is nil")
	} else if ok := timebase.TimeoutAdjustMinute(this.LastTime, this.deviceSvr.OfflineTimeInterval); ok == false { //超时离线
		if this.IsAlarm_OffLine == ALARM_OFF {
			this.deviceSvr.ClearAlarmByMN(this.Station_ID, "发生离线报警", this.LastTime)
		}
		alarmText = fmt.Sprintf("%s_%s_离线报警，请及时处理!", this.LastTime, this.Station_Name)
		this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_OFFLINE, defaultconfig.ALARMNAME_OFFLINE, this.Station_ID, "", "", ALARM_ON, alarmText, defaultconfig.ALARMNAME_OFFLINE, this.LastTime, "", this)
		this.IsAlarm_OffLine = ALARM_ON
		errorlog.ErrorLogDebug("onlinetime", this.devCode, alarmText)
		this.OnLineTime = ""
	} else {
		this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_OFFLINE, defaultconfig.ALARMNAME_OFFLINE, this.Station_ID, "", "", ALARM_OFF, alarmText, "恢复正常", this.LastTime, "", this)
		this.IsAlarm_OffLine = ALARM_OFF
		if this.OnLineTime == "" {
			this.OnLineTime = this.DateTime
			errorlog.ErrorLogDebug("onlinetime", this.devCode, fmt.Sprintf("add time:%s", this.OnLineTime))
		}
	}
}

/** 实时超标判断
 */
func (this *DeviceClient) analyzer_over_real() {
	jsReal, err := this.deviceSvr.redisReal.GetLastData(this.devCode, cfg.ServerProtocol, "2011")
	if err != nil {
		return
	}
	if this.realDT != "" {
		if f, _ := timebase.GetInterval(this.realDT, jsReal.DT, "MINUTE"); f <= 0 {
			fmt.Printf("站点没有更新数据,站点编码=%s 时间=%s\r\n", this.devCode, this.realDT)
			return
		}
	}
	this.realDT = jsReal.DT
	this.IsAlarm_OverReal = ALARM_OFF
	for k, v := range jsReal.HT_Col { //k = Rtd_因子，value = 因子值
		if strings.Index(k, "_") <= 0 { //不符合判断条件
			fmt.Println("不是检测因子", k)
			continue
		}
		fCode := strings.Split(k, "_")[1]
		fMark := strings.Split(k, "_")[0]
		if strings.ToUpper(fMark) != "TAG" {
			continue
		}
		fValue, _ := jsReal.HT_Col[fmt.Sprintf("RTD_%s", fCode)]
		fStand, _ := jsReal.HT_Col[fmt.Sprintf("STAND_%s", fCode)]
		fRatio, _ := jsReal.HT_Col[fmt.Sprintf("RATIO_%s", fCode)]
		if fInfo, ok := this.htFactor[strings.ToUpper(fCode)]; ok == true {
			var alarmText string
			switch v {
			case defaultconfig.DATASTATE_OVER: //超标
				if this.IsAlarm_OffLine == ALARM_ON {
					this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_OVER_REAL, defaultconfig.ALARMNAME_REAL, this.Station_ID, fCode, fInfo.FactorName, ALARM_OFF, alarmText, "恢复正常", this.realDT, fRatio, this)
					this.IsAlarm_OverReal = ALARM_OFF
					continue
				}
				alarmText = fmt.Sprintf("%s_%s_%s(%s)_%s(%s)_实时值超标，请及时处理!", this.realDT, this.Station_Name, fInfo.FactorName, fCode, fValue, fStand)
				this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_OVER_REAL, defaultconfig.ALARMNAME_REAL, this.Station_ID, fCode, fInfo.FactorName, ALARM_ON, alarmText, defaultconfig.ALARMNAME_REAL, this.realDT, fRatio, this)
				this.IsAlarm_OverReal = ALARM_ON
			case defaultconfig.DATASTATE_NORMAL: //正常
				this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_OVER_REAL, defaultconfig.ALARMNAME_REAL, this.Station_ID, fCode, fInfo.FactorName, ALARM_OFF, alarmText, "恢复正常", this.realDT, fRatio, this)
			}
		}
	}
}

//废气呆滞报警判断
func (this *DeviceClient) analyzer_dull_gas(lastState bool) {
	if len(this.GasFactorMap) == 0 {
		return
	}
	if this.OnLineTime == "" {
		errorlog.ErrorLogDebug("dull", this.devCode, fmt.Sprintf("设备为离线状态，不进行恒值判断"))
		return
	}
	for fCode, fInfo := range this.GasFactorMap {
		tableName := fmt.Sprintf("td_raw_2051_%s", this.Station_ID)
		sqlMax := fmt.Sprintf("select * from %s order by DT desc limit %d", tableName, cfg.DullConstant_Gas)
		dtMax, err := this.deviceSvr.mysqlData.SelectSql(sqlMax)
		errorlog.ErrorLogWarn("SQL_ERROR", "false", fmt.Sprintf("%s,%v", sqlMax, err))
		if err != nil {

			continue
		}
		if dtMax.Count <= 1 {
			continue
		}

		et := dtMax.RowData[0]["DT"]
		st := dtMax.RowData[dtMax.Count-1]["DT"]
		if this.factorGasDullMap[fCode] == et { //数据已经判断过
			this.IsAlarm_Dull = lastState
			return
		} else {
			this.factorGasDullMap[fCode] = et
		}
		if f, _ := timebase.GetInterval(st, this.OnLineTime, "MINUTE"); f > 0 {
			errorlog.ErrorLogDebug("dull", this.devCode, fmt.Sprintf("未到恒值判断时间，开始时间=%s,在线时间=%s", st, this.OnLineTime))
			continue
		}
		fstr := strings.Replace(" max(AVG_XXX) as MAX_XXX,min(AVG_XXX) as MIN_XXX, sum(case when AVG_XXX is null then 1 else 0 end) as COUNT_XXX,", "XXX", fInfo.FactorCode, -1)
		fstr = fmt.Sprintf("select %s max(DT) as ET,min(DT) as ST,count(1) as NUM from  %s where DT between '%s' and '%s' ", fstr, tableName, st, et)
		dt, err := this.deviceSvr.mysqlData.SelectSql(fstr)
		errorlog.ErrorLogWarn("SQL_ERROR", "false", fmt.Sprintf("%s,%v", sqlMax, err))
		if err != nil {

			continue
		}
		if dt.Count == 0 {
			continue
		}
		count, _ := strconv.Atoi(dt.RowData[0]["NUM"])
		if count < cfg.DullConstant_Gas {
			continue
		}
		rows := dt.RowData[0]
		this.AnalyzeConstantAlarm(fInfo, dtMax.RowData[0][fmt.Sprintf("TAG_%s", strings.ToUpper(fCode))], rows, st, et, tableName, "2051")
	}
}

//废水呆滞报警判断
func (this *DeviceClient) analyzer_dull_water(lastState bool) {

	if len(this.WaterFactorMap) == 0 {
		return
	}
	if this.OnLineTime == "" {
		errorlog.ErrorLogDebug("dull", this.devCode, fmt.Sprintf("设备为离线状态，不进行恒值判断"))
		return
	}
	for fCode, fInfo := range this.WaterFactorMap {
		tableName := fmt.Sprintf("td_raw_2061_%s", this.Station_ID)
		sqlMax := fmt.Sprintf("select * from %s order by DT desc limit %d", tableName, cfg.DullConstant_Water)
		dtMax, err := this.deviceSvr.mysqlData.SelectSql(sqlMax)
		if err != nil {
			errorlog.ErrorLogWarn("SQL_ERROR", "false", fmt.Sprintf("%s,%v", sqlMax, err))
			continue
		}
		if dtMax.Count <= 1 {
			continue
		}
		et := dtMax.RowData[0]["DT"]
		st := dtMax.RowData[dtMax.Count-1]["DT"]
		if this.factorWaterDullMap[fCode] == et { //数据已经判断过
			this.IsAlarm_Dull = lastState
			return
		} else {
			this.factorWaterDullMap[fCode] = et
		}

		if f, _ := timebase.GetInterval(st, this.OnLineTime, "MINUTE"); f > 0 {
			errorlog.ErrorLogDebug("dull", this.devCode, fmt.Sprintf("未到恒值判断时间，开始时间=%s,在线时间=%s", st, this.OnLineTime))
			continue
		}

		fstr := strings.Replace(" max(AVG_XXX) as MAX_XXX,min(AVG_XXX) as MIN_XXX, sum(case when AVG_XXX is null then 1 else 0 end) as COUNT_XXX,", "XXX", fInfo.FactorCode, -1)
		fstr = fmt.Sprintf("select %s max(DT) as ET,min(DT) as ST,count(1) as NUM from  %s where DT between '%s' and '%s' ", fstr, tableName, st, et)
		dt, err := this.deviceSvr.mysqlData.SelectSql(fstr)
		errorlog.ErrorLogWarn("SQL_ERROR", "false", fmt.Sprintf("%s,%v", sqlMax, err))
		if err != nil {

			continue
		}
		if dt.Count == 0 {
			continue
		}
		count, _ := strconv.Atoi(dt.RowData[0]["NUM"])
		if count < cfg.DullConstant_Water {
			continue
		}
		rows := dt.RowData[0]
		this.AnalyzeConstantAlarm(fInfo, dtMax.RowData[0][fmt.Sprintf("TAG_%s", strings.ToUpper(fCode))], rows, st, et, tableName, "2061")
	}
}

func (this *DeviceClient) AnalyzeConstantAlarm(fInfo *FactorInfo, tag string, rows map[string]string, st string, et string, tableName, cn string) {
	fCode := strings.ToUpper(fInfo.FactorCode)
	max := rows[fmt.Sprintf("MAX_%s", fCode)]
	min := rows[fmt.Sprintf("MIN_%s", fCode)]
	count := rows[fmt.Sprintf("COUNT_%s", fCode)]
	if count != "0" {
		return
	}
	errorlog.ErrorLogDebug("constant", "恒值报警开始判断", fmt.Sprintf("%s,%s[max=%s;min=%s;count=%s]", this.devCode, fInfo.FactorCode, max, min, count))

	alarmTextFinish := fmt.Sprintf("%s_%s%s_监测因子_%s[%s]_恒值报警解除", et, this.EnterInfo.Enterprise_Name, this.Station_Name, fInfo.FactorName, fInfo.FactorCode)

	if tag != defaultconfig.DATASTATE_DULL && tag != defaultconfig.DATASTATE_NORMAL {
		alarmTextFinish = alarmTextFinish + "(高级别报警)" //解除报警
	} else if max == "" || max != min {
		alarmTextFinish = alarmTextFinish + "(数据恢复正常)"
	} else {
		errorlog.ErrorLogDebug("constant", "恒值报警开始", fmt.Sprintf("%s,%s", this.devCode, fInfo.FactorCode))
		//站点状态
		this.IsAlarm_Dull = ALARM_ON

		//报警信息  2019-11-08 10:36:07_万维盈创测试气_数据恒值报警_废气_监测值:50.000
		alarmText := fmt.Sprintf("%s_%s%s_数据恒值报警_%s_监测值:%s", st, this.EnterInfo.Enterprise_Name, this.Station_Name, fInfo.FactorName, max)
		if cn == "2051" {
			//报警
			this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_DULL_MINUTE, defaultconfig.ALARMNAME_DULL_MINUTE, this.Station_ID, fCode, fInfo.FactorName, ALARM_ON, alarmText, defaultconfig.ALARMNAME_DULL_MINUTE, st, "", this)
		} else {
			//报警
			this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_DULL_HOUR, defaultconfig.ALARMNAME_DULL_HOUR, this.Station_ID, fCode, fInfo.FactorName, ALARM_ON, alarmText, defaultconfig.ALARMNAME_DULL_HOUR, st, "", this)
		}

		//修改状态
		this.UpdateFactorTag(fInfo.FactorCode, st, et, tableName)
		return
	}
	if cn == "2051" {
		//结束报警
		this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_DULL_MINUTE, defaultconfig.ALARMNAME_DULL_MINUTE, this.Station_ID, fCode, fInfo.FactorName, ALARM_OFF, alarmTextFinish, "恢复正常", et, "", this)
		errorlog.ErrorLogDebug("constant", "恒值报警结束", fmt.Sprintf("%s,%s,%s", this.devCode, fInfo.FactorCode, alarmTextFinish))
	} else {
		//结束报警
		this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_DULL_HOUR, defaultconfig.ALARMNAME_DULL_HOUR, this.Station_ID, fCode, fInfo.FactorName, ALARM_OFF, alarmTextFinish, "恢复正常", et, "", this)
		errorlog.ErrorLogDebug("constant", "恒值报警结束", fmt.Sprintf("%s,%s,%s", this.devCode, fInfo.FactorCode, alarmTextFinish))
	}
}

/** 故障报警判断
 */
func (this *DeviceClient) analyzer_alarm_fault() {
	var alarmText string
	this.IsAlarm_Fault = ALARM_OFF

	if this.LastTime != "" {
		hour, _ := timebase.GetInterval(this.LastTime, timebase.NowTimeFormat(), "HOUR")
		if hour > this.deviceSvr.FaultTimeInterval {
			this.IsAlarm_Fault = ALARM_ON
		} else {
			this.IsAlarm_Fault = ALARM_OFF
		}
	}
	if this.IsAlarm_Fault == ALARM_ON {
		this.IsAlarm_OffLine = ALARM_ON
		alarmText = fmt.Sprintf("%s_%s_数采仪故障报警，请及时处理!", this.LastTime, this.Station_Name)
		this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_FAULT, defaultconfig.ALARMNAME_FAULT, this.Station_ID, "", "", ALARM_ON, alarmText, defaultconfig.ALARMNAME_FAULT, this.LastTime, "", this)
		//this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_OFFLINE, defaultconfig.ALARMNAME_OFFLINE, this.Station_ID, "", "", ALARM_OFF, alarmText, "数采仪故障", this.LastTime, "", this)
	} else {
		this.deviceSvr.AlarmDispose(defaultconfig.ALARMTYPE_FAULT, defaultconfig.ALARMNAME_FAULT, this.Station_ID, "", "", ALARM_OFF, alarmText, defaultconfig.ALARMNAME_FAULT, this.LastTime, "", this)
	}
}


/** 日数据完整率
 */
func (this *DeviceClient) analyzer_full_all(nowTime time.Time, strST, strED string) {
	_, realInterval := IsNumber(this.TIME_INTERVAL)
	if realInterval <= 0 {
		return
	}
	strsql := fmt.Sprintf("select count(*) as COUNT from %s where DT between '%s' and '%s'", this.TableName_Real, strST, strED)
	dt, err := this.mysqlData.SelectSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "统计日数据完整率结束", fmt.Sprintf("查询失败,%s\n%v", strsql, err))
		return
	}
	fz := 0.0
	for _, rows := range dt.RowData {
		_, fz = IsNumber(rows["COUNT"])
		break
	}
	sHour := 24
	if nowTime.Day() == time.Now().Day() {
		sHour = nowTime.Hour() + 1
	}
	fm := float64(sHour) * 60 * 60 / realInterval

	rate := 0.0
	if fz > 0 && fm > 0 {
		rate = fz * 100 / float64(fm)
		if rate > 100 {
			rate = 100
		}
	}
	mp := make(map[string]string)
	mp["DT"] = strST
	mp["STATION_ID"] = this.StationID
	mp["FENZI"] = fmt.Sprintf("%0.2f", fz)  //实际接受报文条数
	mp["FENMU"] = fmt.Sprintf("%0.2f", fm)  //理论接收报文条数
	mp["RATE"] = fmt.Sprintf("%0.2f", rate) //完整率

	sql, _, err := this.deviceSvr.mysqlBase.DuplicateData("tb_full_rate_day_station", mp)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "false", fmt.Sprintf("入库失败,%s,%v", sql, err))
	}
	errorlog.ErrorLogDebug(this.DevCode, "统计日数据完整率结束", fmt.Sprintf("完整率=%0.2f,分子='%f',分母='%f'", rate, fz, fm))
}

** 因子级别月数据运转率统计
 */
func (this *DeviceClient) analyzer_run_fmonty(nowTime time.Time) {
	strST := timebase.FormatMonth(nowTime)
	strED := timebase.Parse(strST).AddDate(0, 1, 0).Add(-1 * time.Second).Format(timebase.TIME_STA)
	strsql := fmt.Sprintf("SELECT FACTOR_CODE,FACTOR_NAME,SUM(FENZI) AS FZ,SUM(FENMU) AS FM FROM tb_run_rate_day_factor where DT between '%s' and '%s' and STATION_ID='%s' GROUP BY FACTOR_CODE", strST, strED, this.StationID)
	dt, err := this.mysqlBase.SelectSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "统计因子级别月数据运转率结束", fmt.Sprintf("查询失败,%s\n%v", strsql, err))
		return
	}

	var upsql string
	for _, row := range dt.RowData {
		fcode := row["FACTOR_CODE"]
		fname := row["FACTOR_NAME"]
		_, fz := IsNumber(row["FZ"])
		_, fm := IsNumber(row["FM"])
		rate := 0.0
		if fz > 0 && fm > 0 {
			rate = fz * 100 / float64(fm)
			if rate > 100 {
				rate = 100
			}
		}
		upsql += fmt.Sprintf("('%s','%s','%s','%s',%s,%s,%s),", strST, this.StationID, fcode, fname, fmt.Sprintf("%0.2f", fz), fmt.Sprintf("%0.2f", fm), fmt.Sprintf("%0.2f", rate))
	}
	if len(upsql) <= 0 {
		return
	}
	upsql = fmt.Sprintf("REPLACE INTO tb_run_rate_month_factor (DT,STATION_ID,FACTOR_CODE,FACTOR_NAME,FENZI,FENMU,RATE) VALUES %s", upsql[:len(upsql)-1])
	_, errsql := this.deviceSvr.mysqlBase.ExecuteSql(upsql)

	if errsql != nil {
		errorlog.ErrorLogDebug("SQL", "false", fmt.Sprintf("同步错误,%s，%s", upsql, errsql))
	}
}


/** 因子级别年数据运转率统计
 */
func (this *DeviceClient) analyzer_run_fyear(nowTime time.Time) {
	strST := fmt.Sprintf("%d-01-01 00:00:00", nowTime.Year())
	strED := fmt.Sprintf("%d-12-31 23:59:59", nowTime.Year())
	strsql := fmt.Sprintf("SELECT FACTOR_CODE,FACTOR_NAME,SUM(FENZI) AS FZ,SUM(FENMU) AS FM FROM tb_run_rate_month_factor where DT between '%s' and '%s' and STATION_ID='%s' GROUP BY FACTOR_CODE", strST, strED, this.StationID)
	dt, err := this.mysqlBase.SelectSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "统计因子级别年数据运转率结束", fmt.Sprintf("查询失败,%s\n%v", strsql, err))
		return
	}

	var upsql string
	for _, row := range dt.RowData {
		fcode := row["FACTOR_CODE"]
		fname := row["FACTOR_NAME"]
		_, fz := IsNumber(row["FZ"])
		_, fm := IsNumber(row["FM"])
		rate := 0.0
		if fz > 0 && fm > 0 {
			rate = fz * 100 / float64(fm)
			if rate > 100 {
				rate = 100
			}
		}
		upsql += fmt.Sprintf("('%s','%s','%s','%s',%s,%s,%s),", strST, this.StationID, fcode, fname, fmt.Sprintf("%0.2f", fz), fmt.Sprintf("%0.2f", fm), fmt.Sprintf("%0.2f", rate))
	}
	if len(upsql) <= 0 {
		return
	}
	upsql = fmt.Sprintf("REPLACE INTO tb_run_rate_year_factor (DT,STATION_ID,FACTOR_CODE,FACTOR_NAME,FENZI,FENMU,RATE) VALUES %s", upsql[:len(upsql)-1])
	_, errsql := this.deviceSvr.mysqlBase.ExecuteSql(upsql)
	if errsql != nil {
		errorlog.ErrorLogDebug("SQL", "false", fmt.Sprintf("同步错误,%s，%s", upsql, errsql))
	}
}

/**月历史数据统计
根据日数据统计计算 td_raw_2031_stationId
*/
func (this *DeviceClient) analyzer_data_month(nowTime time.Time) {
	strST := timebase.FormatMonth(nowTime)
	strED := timebase.Parse(strST).AddDate(0, 1, 0).Add(-1 * time.Second).Format(timebase.TIME_STA)

	ColumnsReal := this.deviceSvr.mysqlData.GetColumns(this.TableName_Day) //需要统计的表
	ColumnsReal = strings.Replace(ColumnsReal, "][", "],[", -1)
	colList := strings.Split(ColumnsReal, ",")

	var strsql string
	for _, col := range colList {
		col = strings.Replace(col, "[", "", -1)
		col = strings.Replace(col, "]", "", -1)

		if strings.HasPrefix(col, "AVG_") {
			strsql += fmt.Sprintf("AVG(%s) as %s,", col, col)
		}
		if strings.HasPrefix(col, "MAX_") {
			strsql += fmt.Sprintf("MAX(%s) as %s,", col, col)
		}
		if strings.HasPrefix(col, "MIN_") {
			strsql += fmt.Sprintf("MIN(%s) as %s,", col, col)
		}
		if strings.HasPrefix(col, "COU_") {
			strsql += fmt.Sprintf("SUM(%s) as %s,", col, col)
		}

	}
	if len(strsql) < 5 {
		return
	}
	strsql = fmt.Sprintf("select %s from %s where DT between '%s' and '%s'", strsql[:len(strsql)-1], this.TableName_Day, strST, strED)
	dt, err := this.deviceSvr.mysqlData.SelectSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "false", fmt.Sprintf("查询失败,%s,%v", strsql, err))
		return
	}

	mp := make(map[string]string)
	mp["STATION_ID"] = this.StationID
	mp["DEVICE_CODE"] = this.DevCode
	mp["REVDT"] = strST
	mp["DT"] = strST
	for _, rows := range dt.RowData {
		for k, v := range rows {
			if strings.HasPrefix(k, "AVG_") {
				fCode := strings.Split(k, "_")[1]
				tag := this.IsOver(fCode, v)
				mp[fmt.Sprintf("TAG_%s", fCode)] = tag
			}
			mp[k] = v
		}
	}
	upsql, _, uperr := this.deviceSvr.mysqlData.DuplicateData(this.TableName_Month, mp)
	if uperr != nil {
		errorlog.ErrorLogWarn("SQL", "false", fmt.Sprintf("更新,%s,%v", upsql, uperr))
		return
	}
}



/** 排口月在线仪器运转率统计
 */
func (this *DeviceClient) analyzer_run_monty(nowTime time.Time) {
	strST := timebase.FormatMonth(nowTime)
	strED := timebase.Parse(strST).AddDate(0, 1, 0).Add(-1 * time.Second).Format(timebase.TIME_STA)
	strsql := fmt.Sprintf("SELECT SUM(FENZI) AS FZ,SUM(FENMU) AS FM FROM tb_run_rate_day_station where DT between '%s' and '%s' and STATION_ID='%s'", strST, strED, this.StationID)
	dt, err := this.mysqlBase.SelectSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "排口月在线仪器运转率统计结束", fmt.Sprintf("查询失败,%s\n%v", strsql, err))
		return
	}

	fz := 0.0
	fm := 0.0
	for _, rows := range dt.RowData {
		_, fz = IsNumber(rows["FZ"])
		_, fm = IsNumber(rows["FM"])
		break
	}
	rate := 0.0
	if fz > 0 && fm > 0 {
		rate = fz * 100 / float64(fm)
		if rate > 100 {
			rate = 100
		}
	}
	mp := make(map[string]string)
	mp["DT"] = strST
	mp["STATION_ID"] = this.StationID
	mp["FENZI"] = fmt.Sprintf("%0.2f", fz)  //实际接受报文条数
	mp["FENMU"] = fmt.Sprintf("%0.2f", fm)  //理论接收报文条数
	mp["RATE"] = fmt.Sprintf("%0.2f", rate) //完整率

	sql, _, err := this.deviceSvr.mysqlBase.DuplicateData("tb_run_rate_month_station", mp)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "false", fmt.Sprintf("入库失败,%s,%v", sql, err))
	}
	errorlog.ErrorLogDebug(this.DevCode, "排口月在线仪器运转率统计结束", fmt.Sprintf("完整率=%0.2f,分子='%f',分母='%f'", rate, fz, fm))
}

/** 排口年在线仪器运转率统计
 */
func (this *DeviceClient) analyzer_run_year(nowTime time.Time) {
	strST := fmt.Sprintf("%d-01-01 00:00:00", nowTime.Year())
	strED := fmt.Sprintf("%d-12-31 23:59:59", nowTime.Year())
	strsql := fmt.Sprintf("SELECT SUM(FENZI) AS FZ,SUM(FENMU) AS FM FROM tb_run_rate_month_station where DT between '%s' and '%s' and STATION_ID='%s'", strST, strED, this.StationID)
	dt, err := this.mysqlBase.SelectSql(strsql)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "排口年在线仪器运转率统计结束", fmt.Sprintf("查询失败,%s\n%v", strsql, err))
		return
	}
	fz := 0.0
	fm := 0.0
	for _, rows := range dt.RowData {
		_, fz = IsNumber(rows["FZ"])
		_, fm = IsNumber(rows["FM"])
		break
	}
	rate := 0.0
	if fz > 0 && fm > 0 {
		rate = fz * 100 / float64(fm)
		if rate > 100 {
			rate = 100
		}
	}
	mp := make(map[string]string)
	mp["DT"] = strST
	mp["STATION_ID"] = this.StationID
	mp["FENZI"] = fmt.Sprintf("%0.2f", fz)  //实际接受报文条数
	mp["FENMU"] = fmt.Sprintf("%0.2f", fm)  //理论接收报文条数
	mp["RATE"] = fmt.Sprintf("%0.2f", rate) //完整率

	sql, _, err := this.deviceSvr.mysqlBase.DuplicateData("tb_run_rate_year_station", mp)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "false", fmt.Sprintf("入库失败,%s,%v", sql, err))
	}
	errorlog.ErrorLogDebug(this.DevCode, "排口年在线仪器运转率统计结束", fmt.Sprintf("完整率=%0.2f,分子='%f',分母='%f'", rate, fz, fm))
}

/** 数据同步
 */
func (this *DeviceClient) sync_table(tabName, CN string) int {
	if this.htFactor == nil {
		fmt.Println("因子列表为空", this.DeviceCode)
		return 0
	}
	//1、查看表是否存在,不存在直接退出
	if ok := this.mysqlD.TabExist(tabName); ok == false {
		return 0
	}
	ColList := strings.ToUpper(this.mysqlD.GetColumns(tabName)) //获取因子列表
	count := this.deviceSvr.redisParse.GetDataLen(this.DeviceCode, CN, cfg.ServerProtocol)
	for i := 1; i <= count; i++ {
		js := this.deviceSvr.redisParse.GetData(this.DeviceCode, CN, cfg.ServerProtocol)
		if js != nil {
			mp := make(map[string]string)

			for col, _ := range js.HT_Col {
				if strings.Contains(ColList, strings.ToUpper(col)) {
					mp[col] = js.HT_Col[col]
				}
			}
			mp["STATION_ID"] = this.StationID
			mp["DEVICE_CODE"] = this.DeviceCode
			mp["DT"] = js.DT
			mp["REVDT"] = js.RevDT
			strsql, _, err := this.mysqlD.DuplicateData(tabName, mp)
			if err != nil {
				errorlog.ErrorLogDebug("SQL", "false", fmt.Sprintf("同步错误,%s，%s", strsql, err))
			} else {
				errorlog.ErrorLogDebug("SQL", "true", fmt.Sprintf("同步成功,%s", strsql))
			}
		}
	}
	return count
}

/**
overValue = 超标倍数，只有超标非正常数据表需要写入
*/
func (this *DeviceClient) Insert_abnormal_Table(tabName, OverCode string) int {

	//1、查看表是否存在,不存在直接退出
	if ok := this.mysqlD.TabExist(tabName); ok == false {
		return 0
	}
	count := this.deviceSvr.redisParse.GetAlarmFactorLen(this.DeviceCode, OverCode)
	for i := 1; i <= count; i++ {
		js := this.deviceSvr.redisParse.GetAlarmFactor(this.DeviceCode, OverCode)
		if js != nil {
			mp := make(map[string]string)
			mp["STATION_ID"] = this.StationID
			mp["DT"] = js.DT
			mp["FACTOR_CODE"] = js.FactorCode
			mp["FACTOR_NAME"] = js.FactorName
			mp["UP_VALUE"] = js.UpValue
			mp["LOW_VALUE"] = js.LowValue
			mp["REAL_VALUE"] = js.FactorValue
			mp["FACTOR_UNIT"] = js.Cou_Unit
			mp["RELIEVE_STATE"] = "0"
			mp["OVERPROOF_TIMES"] = js.ValueRatio
			mp["DATA_TYPE"] = js.CN
			mp["EXCEED_TIME"] = ""
			switch js.CN {
			case "2031":
				mp["DATA_TYPE"] = "2031"
			case "2051":
				mp["DATA_TYPE"] = "2051"
			case "2061":
				mp["DATA_TYPE"] = "2061"
			}
			strsql, _, err := this.mysqlD.DuplicateData(this.TableName_Over, mp)
			if err != nil {
				errorlog.ErrorLogDebug("SQL", "false", fmt.Sprintf("同步错误,%s，%s", strsql, err))
			} else {
				errorlog.ErrorLogDebug("SQL", "true", fmt.Sprintf("同步成功,%s", strsql))
			}
		}
	}
	return count
}

/** 数据表处理
字段列提前准备好，直接判断写入
*/
func (this *DeviceClient) analyzer_table(tabName string, CN string) {
	if this.htFactor == nil {
		fmt.Println("因子列表为空", this.Device_Code)
		return
	}
	this.deviceSvr.CreateTableInData(tabName, TABLEPATH_Raw)
	ColList := this.mysqlD.GetColumns(tabName)
	mp := make(map[string]string)
	for _, fInfo := range this.htFactor {
		if fInfo.RealCols == nil {
			return
		}
		if CN == "2011" {
			for colName, colType := range fInfo.RealCols {
				if ok := strings.Contains(ColList, colName); ok == false { //未知因子需要添加
					mp[colName] = colType
				}
			}
		} else {
			for colName, colType := range fInfo.HisCols {
				if ok := strings.Contains(ColList, colName); ok == false { //未知因子需要添加
					mp[colName] = colType
				}
			}
		}
	}

	if len(mp) > 0 {
		this.mysqlD.CreateColumns(tabName, mp)
	}
}

/** 更新状态
1、从mysql中读取最新一条数据
2、判断排口状态
*/
func (this *DeviceClient) analyzer_UpdateState() {

	js := new(JsonRealState)
	js.DeviceCode = this.devCode
	js.DeviceAddr = this.RemoteAddr
	js.LocalAddr = this.LocalAddr
	js.StateDT = timebase.NowTimeFormat()
	js.DT = this.LastTime
	js.ProType = cfg.ServerProtocol

	mp := make(map[string]string)

	switch {
	//case this.IsAlarm_Fault:
	//	js.AlarmType = defaultconfig.DATASTATE_FAULT
	case this.IsAlarm_OffLine:
		js.AlarmType = defaultconfig.DATASTATE_OFFLINE
	case this.IsAlarm_OverReal:
		js.AlarmType = defaultconfig.DATASTATE_OVER
	//case this.IsAlarm_OverMinute:
	//	js.AlarmType = defaultconfig.DATASTATE_OVER
	//case this.IsAlarm_OverHour:
	//	js.AlarmType = defaultconfig.DATASTATE_OVER
	case this.IsAlarm_Dull:
		js.AlarmType = defaultconfig.DATASTATE_DULL
	default:
		js.AlarmType = defaultconfig.DATASTATE_NORMAL
	}

	this.deviceSvr.redisState.UpdateDeviceState(this.Station_ID, js)

	mp["OFFLINE_TIME"] = this.LastTime
	mp["STATION_ID"] = this.Station_ID
	mp["STATE"] = js.AlarmType
	mp["ISONLINE"] = "1"
	if js.AlarmType == defaultconfig.DATASTATE_OFFLINE {
		mp["ISONLINE"] = js.AlarmType
	}
	sqls, _, err := this.deviceSvr.mysqlBase.DuplicateData("td_device_state", mp)
	if err != nil {
		errorlog.ErrorLogWarn("SQL", "false", fmt.Sprintf("插入失败,%s,%v", sqls, err))
	}
}

