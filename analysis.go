package main

import (
	"bufio"
	"flag"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"time"
)

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
	data digData
	uid  string
}

// 做信息存储
type urlNode struct {
}

type storageBlock struct {
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
	logFilePath := flag.String("logFilePath", "/User/pangee/Public/nginx/log/dig.log", "log file path")
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

	// 日志消费
	go readFileLinebyLine(params, logChannel)
	// 创建一组日志处理
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}
	// 创建PV UV统计器
	go pvCounter(pvChannel, storageChannel)
	go uvCounter(uvChannel, storageChannel)
	// 如果还要加统计，可以在这里直接加

	// 创建存储器
	go dataStorage(storageChannel)
	time.Sleep(1000 * time.Second)
}

func dataStorage(storageChannel chan storageBlock) {

}

// 页面访问量
func pvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {

}

// 用户访问量，需要根据用户id去重
func uvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {

}

func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) {

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
