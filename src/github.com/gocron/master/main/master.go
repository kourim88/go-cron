package main

import (
	"runtime"
	"github.com/gocron/master"
	"fmt"
	"flag"
	"time"
)

var (
	confFile string
)

// コマンドを解析
func initArgs() {
	flag.StringVar(&confFile, "config", "./package.json", "package.json")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)
	// コマンドArgsを初期化
	initArgs()

	initEnv()

	// Configファイルを解析
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// Workerモジュールを初期化
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	// ログ管理
	if err =master.InitLogMgr(); err != nil {
		goto ERR
	}

	//  タスク管理
	if err = master.InitTaskMgr(); err != nil {
		goto ERR
	}

	// Http Apiサービスを起動
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	return

ERR:
	fmt.Println(err)
}
