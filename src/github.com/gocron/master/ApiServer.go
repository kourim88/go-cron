package master

import (
	"net/http"
	"net"
	"time"
	"strconv"
	"github.com/gocron/common"
	"encoding/json"
)

// タスクのHTTPインターフェース
type ApiServer struct {
	httpServer *http.Server
}

var (
	Sg_apiServer *ApiServer
)

// タスク保存処理
func handleTaskSave(res http.ResponseWriter, req *http.Request) {
	var (
		err error
		postTask string
		task common.Task
		oldTask *common.Task
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	postTask = req.PostForm.Get("task")
	if err = json.Unmarshal([]byte(postTask), &task); err != nil {
		goto ERR
	}
	if oldTask, err = Sg_taskMgr.SaveTask(&task); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", oldTask); err == nil {
		if _, err = res.Write(bytes); err != nil {
			goto ERR
		}
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

// タスク削除処理
func handleTaskDelete(res http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		oldTask *common.Task
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 削除するタスク名
	name = req.PostForm.Get("name")

	// タスク削除する
	if oldTask, err = Sg_taskMgr.DeleteTask(name); err != nil {
		goto ERR
	}

	// 正常なResponse
	if bytes, err = common.BuildResponse(0, "success", oldTask); err == nil {
		res.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

// crontabタスクリスト処理
func handleTaskList(res http.ResponseWriter, req *http.Request) {
	var (
		taskList []*common.Task
		bytes []byte
		err error
	)
	res.Header().Set("Content-Type", "application/json")

	if taskList, err = Sg_taskMgr.ListTasks(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", taskList); err == nil {
		res.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

// あるタスクを強制的にKill
func handleTaskKill(res http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.PostForm.Get("name")

	if err = Sg_taskMgr.KillTask(name); err != nil {
		goto ERR
	}

	res.Header().Set("Content-Type", "application/json")

	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		res.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

// タスクLog詳細処理
func handleTaskLog(res http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		skipParam string
		limitParam string
		skip int
		limit int
		logArr []*common.TaskLog
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}

	if logArr, err = Sg_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	res.Header().Set("Content-Type", "application/json")

	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		res.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

// 正常なWorker　Nodeを所得処理
func handleWorkerList(res http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err error
		bytes []byte
	)

	if workerArr, err = Sg_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	res.Header().Set("Content-Type", "application/json")

	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		res.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

// 初期化サーバ
func InitApiServer() (err error){
	var (
		serveMux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir
		staticHandler http.Handler
	)

	// ルータをセッティング
	serveMux = http.NewServeMux()
	serveMux.HandleFunc("/task/save", handleTaskSave)
	serveMux.HandleFunc("/task/delete", handleTaskDelete)
	serveMux.HandleFunc("/task/list", handleTaskList)
	serveMux.HandleFunc("/task/kill", handleTaskKill)
	serveMux.HandleFunc("/task/log", handleTaskLog)
	serveMux.HandleFunc("/worker/list", handleWorkerList)

	staticDir = http.Dir(Sg_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	serveMux.Handle("/", http.StripPrefix("/", staticHandler))

	if listener, err = net.Listen("tcp", ":" + strconv.Itoa(Sg_config.ApiPort)); err != nil {
		return
	}

	httpServer = &http.Server{
		ReadTimeout: time.Duration(Sg_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(Sg_config.ApiWriteTimeout) * time.Millisecond,
		Handler: serveMux,
	}

	Sg_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	go httpServer.Serve(listener)

	return
}