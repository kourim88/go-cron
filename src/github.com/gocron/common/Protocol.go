package common

import (
	"encoding/json"
	"strings"
	"github.com/gorhill/cronexpr"
	"time"
	"context"
)

// crontabタスク
type Task struct {
	Name string `json:"name"`
	Command string	`json:"command"`
	CronExpr string	`json:"cronExpr"`
}

type TaskSchedulePlan struct {
	Task *Task
	Expr *cronexpr.Expression
	NextTime time.Time
}

type TaskExecuteInfo struct {
	Task *Task
	PlanTime time.Time
	RealTime time.Time
	CancelCtx context.Context
	CancelFunc context.CancelFunc
}

// HTTPレスポンス
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

type TaskEvent struct {
	EventType int
	Task *Task
}

type TaskExecuteResult struct {
	ExecuteInfo *TaskExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

type TaskLog struct {
	TaskName string `json:"taskName" bson:"taskName"`
	Command string `json:"command" bson:"command"`
	Err string `json:"err" bson:"err"`
	Output string `json:"output" bson:"output"`
	PlanTime int64 `json:"planTime" bson:"planTime"`
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"`
	StartTime int64 `json:"startTime" bson:"startTime"`
	EndTime int64 `json:"endTime" bson:"endTime"`
}

type LogBatch struct {
	Logs []interface{}
}

type TaskLogFilter struct {
	TaskName string `bson:"taskName"`
}

// ログ　ソート
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"`
}

func BuildResponse(errno int, msg string, data interface{}) (res []byte, err error) {
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	res, err = json.Marshal(response)
	return
}

func UnpackTask(value []byte) (ret *Task, err error) {
	var (
		task *Task
	)

	task = &Task{}
	if err = json.Unmarshal(value, task); err != nil {
		return
	}
	ret = task
	return
}

func ExtractTaskName(taskKey string) string {
	return strings.TrimPrefix(taskKey, TASK_SAVE_DIR)
}

func ExtractKillerName(killerKey string) string {
	return strings.TrimPrefix(killerKey, TASK_KILLER_DIR)
}

func BuildTaskEvent(eventType int, task *Task) (taskEvent *TaskEvent) {
	return &TaskEvent{
		EventType: eventType,
		Task: task,
	}
}

func BuildTaskSchedulePlan(task *Task) (taskSchedulePlan *TaskSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	if expr, err = cronexpr.Parse(task.CronExpr); err != nil {
		return
	}

	taskSchedulePlan = &TaskSchedulePlan{
		Task: task,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

func BuildTaskExecuteInfo(taskSchedulePlan *TaskSchedulePlan) (taskExecuteInfo *TaskExecuteInfo){
	taskExecuteInfo = &TaskExecuteInfo{
		Task: taskSchedulePlan.Task,
		PlanTime: taskSchedulePlan.NextTime,
		RealTime: time.Now(),
	}
	taskExecuteInfo.CancelCtx, taskExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

func ExtractWorkerIP(regKey string) string {
	return strings.TrimPrefix(regKey, TASK_WORKER_DIR)
}
