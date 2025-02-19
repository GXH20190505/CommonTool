package GranfaBase

import (
	"errorlog"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)
/**根据要监控的不同逻辑定义不同的名称保存所有的要监控对象
 */
type GraType struct {
	Server   http.Handler
  ProType	 PrometheusType//收集器类型
  Counter    map[string]prometheus.Counter//key为定义的收集器名称不可重复
  Gauge      map[string] prometheus.Gauge
  Histogram  map[string]prometheus.Histogram
  Summary    map[string]prometheus.Summary
}
/**
在Addcount方法中可以指定当前要添加的收集器类行
*/
type PrometheusType struct {
	Counter   string
	Gauge     string
	Histogram string
	Summary   string
}
/**
收集器的属性结构体，counter,Gauge 使用1,2,3项，histogram使用1,2,3，4,5,6项 ，summary使用1,2,3,7项
*/
type CollectAttu struct {
	Namespace   string
	Name        string
	Help        string
	BucketStart float64
	BucketWidth float64
	BucketCount int //如果使用histogram 此变量必须大于0
	Objectives  map[float64]float64//{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
}
/**
初始化获得总的Granfa集合*/
func NewGranfaCache() (*GraType, error) {
	var err error
	Gt:=new(GraType)
	Gt.ProType=PrometheusType{
		Counter: "Counter",
		Gauge: "Gauge",
		Histogram: "Histogram",
		Summary: "Summary",
	}
	Gt.Counter=make(map[string]prometheus.Counter)
	Gt.Gauge=make(map[string]prometheus.Gauge)
	Gt.Histogram=make(map[string]prometheus.Histogram)
	Gt.Summary=make(map[string]prometheus.Summary)
	return Gt, err

}

/**
Granfa以那些端口开始运行，数据保存在那些路径下（建议以项目名称作为标识）*/
func (g *GraType) Listen(path,port string)  {
	if port==""{
		port="8080"
	}
	if path==""{
		path="metrics"
	}
	http.Handle(fmt.Sprintf("/%s",path), promhttp.Handler())
	http.ListenAndServe(fmt.Sprintf(":%s",port),nil)
}


/**向收集器添加数据方法*/
func (g *GraType) CounterAdd(name string,value float64)  {
	if _,ok:=g.Counter[name];ok{
		g.Counter[name].Add(value)
	}
}
func (g *GraType) GaugeAdd(name string,value float64)  {
	if _,ok:=g.Gauge[name];ok{
		g.Gauge[name].Set(value)
	}
}
func (g *GraType) HistogramAdd(name string,value float64)  {
	if _,ok:=g.Histogram[name];ok{
		g.Histogram[name].Observe(value)
	}
}
func (g *GraType) SummaryAdd(name string,value float64)  {
	if _,ok:=g.Summary[name];ok{
		g.Summary[name].Observe(value)
	}
}



/**
创建收集器*/
func (g *GraType) AddCollector(typ string,attu CollectAttu) bool {
	switch typ {
	case "Counter":
		counter:= prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: attu.Namespace,
			Name:attu.Name,
			Help: attu.Help,
		})
		g.Counter[attu.Name]=counter
	    return 	g.mustRegister(counter)
	case "Gauge":
		Gauge:= prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: attu.Namespace,
			Name:attu.Name,
			Help: attu.Help,
			ConstLabels: map[string]string{
				"path":fmt.Sprintf("api/gauge/%s",attu.Name),
			},
		})
		g.Gauge[attu.Name]=Gauge
		return g.mustRegister(Gauge)
	case "Histogram":
		histogram:=prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: attu.Namespace,
			Name:attu.Name,
			Help: attu.Help,
			Buckets: prometheus.LinearBuckets(attu.BucketStart,attu.BucketWidth,attu.BucketCount),
			ConstLabels: map[string]string{
				"path":fmt.Sprintf("api/histogram/%s",attu.Name),
			},
		})
		g.Histogram[attu.Name]=histogram
		return g.mustRegister(histogram)
	case "Summary":
		summary:=prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: attu.Namespace,
			Name:attu.Name,
			Help: attu.Help,
			Objectives: attu.Objectives,//map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: map[string]string{
				"path":fmt.Sprintf("api/summary/%s",attu.Name),
			},
		})
		g.Summary[attu.Name]=summary
		return g.mustRegister(summary)
	default:
		return false
	}
}

/** MustRegister将声明的接口类型进行注册
 */
func (g *GraType) mustRegister(c prometheus.Collector)bool {
	err:=prometheus.Register(c)
	if err!=nil{
		errorlog.ErrorLogWarn("Granfa","register",err.Error())
		return  false
	}
	return true
}

/*调试使用
func (g *GraType) AllRegist()map[int]string{
	failRegist:=make(map[int]string)//记录未注册成功的名称
	count:=0
	if len(g.Counter)>0{
		for s, counter := range g.Counter {
			count++
			if !g.mustRegister(counter){
				failRegist[count]=s
				continue
			}
		}
	}
	if len(g.Gauge)>0{
		for s, counter := range g.Gauge {
			count++
			if !g.mustRegister(counter){
				failRegist[count]=s
				continue
			}
		}
	}
	if len(g.Histogram)>0{
		for s, counter := range g.Histogram {
			count++
			if !g.mustRegister(counter){
				failRegist[count]=s
				continue
			}
		}
	}
	if len(g.Summary)>0{
		count++
		for s, counter := range g.Summary {
			if !g.mustRegister(counter){
				failRegist[count]=s
				continue
			}
		}
	}
	return failRegist
}*/


