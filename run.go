package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"time"
	//"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type resource struct {
	url    string
	target string
	start  int
	end    int
}

func ruleResource() []resource {
	var res []resource
	r1 := resource{ // 首页
		url:    "http://localhost:8888/",
		target: "",
		start:  0,
		end:    0,
	}
	// 数据库里面有21条
	r2 := resource{ // 列表页
		url:    "http://localhost:8888/list/{$id}.html",
		target: "{$id}",
		start:  1,
		end:    21,
	}
	r3 := resource{ // 详情页
		url:    "http://localhost:8888/movie/${id}.html",
		target: "{$id}",
		start:  1,
		end:    12924,
	}
	res = append(append(append(res, r1), r2), r3)
	return res
}

// 把数据库里的信息按照这个模板对应的生成url
func buildUrl(res []resource) []string {
	var list []string
	for _, resItem := range res { // 第一个是索引，第二个是值
		if len(resItem.target) == 0 {
			list = append(list, resItem.url)
		} else {
			for i := 0; i <= resItem.end; i++ {
				urlStr := strings.Replace(resItem.url, resItem.target, strconv.Itoa(i), -1) // 最后一个参数-1代表替换全部
				list = append(list, urlStr)
			}
		}
	}
	return list
}
func makeLog(current, refer, ua string) string {
	u := url.Values{}
	u.Set("time", "1")
	u.Set("url", current)
	u.Set("refer", refer)
	u.Set("ua", ua)
	paramsStr := u.Encode()
	logTemplate := "127.0.0.1/dig?{paramsStr}{$ua}"
	log := strings.Replace(logTemplate, "{$paramsStr}", paramsStr, -1)
	log = strings.Replace(log, "{$ua}", ua, -1)
	return log
}

func randInt(min, max int) int { // 在一个区间选一个随机数
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // 中间传的是种子值，不传的话后面生成随机数就会重复
	if min > max {
		return max
	}
	//Intn 从默认 Source 返回半开区间 [0,n) 中的非负伪随机数，作为 int。如果 n <= 0，它会Panics
	return r.Intn(max-min) + min // 防止出现负数
}

func main() {
	// 这个run.go文件可以根据自己设置的数量自动生成数据，方便后面统计时满足大量数据的统计要求，批量生成日志
	uaList := []string{""}
	total := flag.Int("total", 100, "how many rows by created")
	filePath := flag.String("filePath", "/Users/pangee/Public/nginx/logs/dig.log", "log file path")
	flag.Parse()
	fmt.Println(*total, *filePath)

	// 需要构造出真实的网站url集合
	res := ruleResource()
	list := buildUrl(res)
	fmt.Println(list)
	// 按照要求生成$total行日志内容，源自这个集合
	logStr := ""
	for i := 0; i < *total; i++ {
		currentUrl := list[randInt(0, len(list)-1)]
		referUrl := list[randInt(0, len(list)-1)]
		ua := uaList[randInt(0, len(uaList)-1)]
		logStr += makeLog(currentUrl, referUrl, ua) + "\n"

		// 写入文件操作
		//os.WriteFile(*filePath, []byte(logStr), 0644)
		// 上面这个会覆盖写，改成下面的追加写
	}
	fd, _ := os.OpenFile(*filePath, os.O_RDWR|os.O_APPEND, 0644)
	fd.Write([]byte(logStr))
	fd.Close()
	fmt.Println("done.\n")
}
