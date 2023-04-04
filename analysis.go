package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const HANDLE_DIG = " /dig?"
const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST = "/list/"
const HANDLE_HTML = ".html"

type cmdParams struct {
	logFilePath string
	routineNum  int
}

type digData struct { // 对应js上报的数据格式
	time  string
	url   string
	refer string
	ua    string
}

// 做信息传输
type urlData struct {
	data  digData
	uid   string
	unode urlNode
}

// 做信息存储
type urlNode struct {
	unType string // 详情页 或者 列表页 或者 首页
	unRid  int    // Resource ID 资源ID
	unUrl  string // 当前这个页面的url
	unTime string // 当前访问这个页面的时间
}

type storageBlock struct { // 用于存储的结构体
	counterType  string  // 区分是什么统计
	storageModel string  // 存什么样的格式，以什么形式存储
	unode        urlNode // 存储内容
}

var log = logrus.New()

func init() {
	// 这里配置第三方日志库logrus的配置
	log.Out = os.Stdout             // 设置输出
	log.SetLevel(logrus.DebugLevel) // 设置等级
}

func main() {
	// 获取参数
	logFilePath := flag.String("logFilePath", "/Users/pangee/Public/nginx/log/dig.log", "log file path")
	// 设置并发数
	routineNum := flag.Int("routineNum", 5, "consumer number by go routine") // 如果不传的话默认是5
	// 设置日志存放的地方
	l := flag.String("l", "/tmp/log", "this program runtime log target file path")
	flag.Parse()
	params := cmdParams{
		*logFilePath,
		*routineNum,
	}

	// 打日志
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		log.Out = logFd // 获取了文件
		defer logFd.Close()
	}
	log.Infof("Exec start")
	log.Infof("Params: logFilePath=%s,routineNum=%d", params.logFilePath, params.routineNum)

	// 初始化一些channel，用于数据传递
	var logChannel = make(chan string, 3*params.routineNum) // 因为获取消费日志时数据比较大，所以空间*3翻倍
	var pvChannel = make(chan urlData, params.routineNum)
	var uvChannel = make(chan urlData, params.routineNum)
	var storageChannel = make(chan storageBlock, params.routineNum)

	// Redis pool 连接池
	redisPool, err := pool.New("tcp", "localhost:6379", 2*params.routineNum) // 第三个参数是多大的并发度
	if err != nil {
		log.Fatalln("Redis pool created failed")
		panic(err)
	} else {
		go func() {
			for { // 防止连接池断开
				redisPool.Cmd("PING")
				time.Sleep(3 * time.Second)
			}
		}()
	}

	// 日志消费
	go readFileLinebyLine(params, logChannel)
	// 创建一组日志处理
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}
	// 创建PV UV统计器
	go pvCounter(pvChannel, storageChannel)
	go uvCounter(uvChannel, storageChannel, redisPool)
	// 如果还要加统计，可以在这里直接加

	// 创建存储器
	go dataStorage(storageChannel, redisPool)
	time.Sleep(1000 * time.Second)
}

// 把uv和pv的数据存储起来
func dataStorage(storageChannel chan storageBlock, redisPool *pool.Pool) {
	for block := range storageChannel { // 遍历每一个传递过来的消息
		prefix := block.counterType + "_" // 区分是什么统计
		// 10000 * N * M =
		// 逐层添加，加洋葱皮的过程
		// 维度： 天、小时、分钟
		// 层级：定级-大分类-小分类-终极页面
		// 存储模型：Redis SortedSet
		setKeys := []string{ // 把每一个消息按照时分秒分类，添加到对应的redis集合中
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" + getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" + getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" + getTime(block.unode.unTime, "min"),
		}
		rowId := block.unode.unRid    // 路由里面的123.html数字编号
		for _, key := range setKeys { // 遍历上面的切片
			ret, err := redisPool.Cmd(block.storageModel, key, 1, rowId).Int() // int()表示从结果中提取整数值
			if ret <= 0 || err != nil {
				log.Errorln("DataStorage redis storage error.", block.storageModel, key, rowId)
			} // 成功就不做输出
		}
	}
}

// 页面访问量
func pvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range pvChannel {
		SItem := storageBlock{"pv", "ZINCRBY", data.unode}
		storageChannel <- SItem
	}
}

// 用户访问量，需要根据用户id去重
func uvCounter(uvChannel chan urlData, storageChannel chan storageBlock, redisPool *pool.Pool) {
	for data := range uvChannel {
		// HyperLoglog redis 行业一般用这个统计
		// uv一般都是按天去重
		hyperLogLogKey := "uv_hpll_" + getTime(data.data.time, "day")
		// Redis PFADD命令将所有元素的参数保存在指定为第一个参数的键名的HyperLogLog数据结构
		ret, err := redisPool.Cmd("PFADD", hyperLogLogKey, data.uid, "EX", 86400).Int()
		if err != nil {
			log.Warningln("UvCounter check redis hyperloglog failed")
		}
		if ret != 1 { // 执行命令的状态
			continue
		}
		sItem := storageBlock{"uv", "ZINCRBY", data.unode}
		storageChannel <- sItem
	}
}

func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) error {
	for logStr := range logChannel {
		// 切割日志字符串，扣出打点上报的数据
		data := cutLogFetchData(logStr) // 把需要的信息抽取出来
		// uid
		// 说明：课程模拟生成uid，md5(refer+ua)
		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil)) // 转成字符串
		// 很多解析的工作可以放到这里完成
		// ...
		uData := urlData{data, uid, formatUrl(data.url, data.time)}

		//log.Infoln(uData) //打印一条带有INFO级别的日志，并自动在结尾加上一个换行符

		pvChannel <- uData
		uvChannel <- uData
	}
	return nil
}

func cutLogFetchData(logStr string) digData { // 从每一条的日志中获取想要的结构
	logStr = strings.TrimSpace(logStr) // 去除前后空格
	pos1 := str.IndexOf(logStr, HANDLE_DIG, 0)
	if pos1 == -1 { //没找到
		return digData{}
	}
	pos1 += len(HANDLE_DIG)                             // 找到的话偏移量加上文本长度
	pos2 := str.IndexOf(logStr, " HTTP/", pos1)         // 从pos1开始找
	d := str.Substr(logStr, pos1, pos2-pos1)            // 把字符串截取出来
	urlInfo, err := url.Parse("http://localhost/?" + d) // 因为url.Parse只能解析网址，所以要拼成完整的http://xxxx/?xxxx=xxx&xxx=xxx
	if err != nil {
		return digData{}
	}
	data := urlInfo.Query() // 解析query参数
	return digData{         // 上报信息被解析
		data.Get("time"),
		data.Get("refer"),
		data.Get("url"),
		data.Get("ua"),
	}
}

func readFileLinebyLine(params cmdParams, logChannel chan string) error {
	fd, err := os.Open(params.logFilePath) // 打开这个路径下的文件
	if err != nil {
		log.Warningf("ReadFileLinebyLine can'y open file:%s", params.logFilePath)
		return err
	}
	defer fd.Close()                  // 打开文件记得关
	bufferRead := bufio.NewReader(fd) // 带缓冲的读，要比直接os的读取要更快
	count := 0
	for {
		// 从缓冲区中读取字符串。它的作用是从缓冲区中读取指定长度的字符串，直到读取到指定的分隔符或者缓冲区的结尾
		line, err := bufferRead.ReadString('\n')
		logChannel <- line
		log.Infof("line:" + line)
		count++
		if count%(1000*params.routineNum) == 0 {
			log.Infof("ReadFilebyLine line:%d", count)
		}
		if err != nil {
			if err == io.EOF { // 全部消费完了
				time.Sleep(3 * time.Second)
				log.Infof("ReadFileLinebyLine wait,readline:%d", count)
			} else {
				// 说明不是读取完了，而是读取中出错了
				log.Warningf("")
			}
		}
	}
	return nil
}

func formatUrl(url, t string) urlNode {
	// 一定从量大的着手，详情页>列表页>首页
	pos1 := str.IndexOf(url, HANDLE_MOVIE, 0) // 字符串扣取
	if pos1 != -1 {                           // url里面包含HANDLE_MOVIE
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url, HANDLE_HTML, 0)
		idStr := str.Substr(url, pos1, pos2-pos1)
		id, _ := strconv.Atoi(idStr) // 将字符串转成数字
		return urlNode{"movie", id, url, t}
	} else { // 列表页
		pos1 = str.IndexOf(url, HANDLE_LIST, 0)
		if pos1 != -1 {
			pos1 += len(HANDLE_LIST)
			pos2 := str.IndexOf(url, HANDLE_HTML, 0)
			idStr := str.Substr(url, pos1, pos2-pos1)
			id, _ := strconv.Atoi(idStr) // 将字符串转成数字
			return urlNode{"list", id, url, t}
		} else { // 首页
			return urlNode{"home", 1, url, t}
		} // 如果页面url有很多种，就不断在这里扩展
	}
}

func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
		break
	case "hour":
		item = "2006-01-02 15"
		break
	case "min":
		item = "2006-01-02 15:04"
		break
	}
	t, _ := time.Parse(item, time.Now().Format(item)) //time.Parse是Go语言中用于将字符串转换为时间类型的函数
	return strconv.FormatInt(t.Unix(), 10)            // 将整数转换成字符串形式
}
