package worker

import (
	"fmt"
	"github.com/gocron/common"
	"time"
)

type Scheduler struct {
	taskEventChan chan *common.TaskEvent
	taskPlanTable map[string]*common.TaskSchedulePlan
	taskExecutingTable map[string]*common.TaskExecuteInfo
	taskResultChan chan *common.TaskExecuteResult
}

var (
	Sg_scheduler *Scheduler
)

func (scheduler *Scheduler) handleTaskEvent(taskEvent *common.TaskEvent) {
	var (
		taskSchedulePlan *common.TaskSchedulePlan
		taskExecuteInfo *common.TaskExecuteInfo
		taskExecuting bool
		taskExisted bool
		err error
	)
	switch taskEvent.EventType {
	case common.TASK_EVENT_SAVE:
		if taskSchedulePlan, err = common.BuildTaskSchedulePlan(taskEvent.Task); err != nil {
			return
		}
		scheduler.taskPlanTable[taskEvent.Task.Name] = taskSchedulePlan
	case common.TASK_EVENT_DELETE:
		if taskSchedulePlan, taskExisted = scheduler.taskPlanTable[taskEvent.Task.Name]; taskExisted {
			delete(scheduler.taskPlanTable, taskEvent.Task.Name)
		}
	case common.TASK_EVENT_KILL:
		if taskExecuteInfo, taskExecuting = scheduler.taskExecutingTable[taskEvent.Task.Name]; taskExecuting {
			taskExecuteInfo.CancelFunc()
		}
	}
}

func (scheduler *Scheduler) TryStartTask(taskPlan *common.TaskSchedulePlan) {
	var (
		taskExecuteInfo *common.TaskExecuteInfo
		taskExecuting bool
	)

	if taskExecuteInfo, taskExecuting = scheduler.taskExecutingTable[taskPlan.Task.Name]; taskExecuting {
		return
	}

	taskExecuteInfo = common.BuildTaskExecuteInfo(taskPlan)

	scheduler.taskExecutingTable[taskPlan.Task.Name] = taskExecuteInfo

	fmt.Println("タスクの実行:", taskExecuteInfo.Task.Name, taskExecuteInfo.PlanTime, taskExecuteInfo.RealTime)
	Sg_executor.ExecuteTask(taskExecuteInfo)
}

func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		taskPlan *common.TaskSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	if len(scheduler.taskPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	now = time.Now()

	// 全てのタスクをループ
	for _, taskPlan = range scheduler.taskPlanTable {
		if taskPlan.NextTime.Before(now) || taskPlan.NextTime.Equal(now) {
			scheduler.TryStartTask(taskPlan)
			// 次回の実行時間を更新する
			taskPlan.NextTime = taskPlan.Expr.Next(now)
		}

		// 有効期限が切れる最新のタスク時間を計算する
		if nearTime == nil || taskPlan.NextTime.Before(*nearTime) {
			nearTime = &taskPlan.NextTime
		}
	}
	scheduleAfter = (*nearTime).Sub(now)
	return
}

// タスク処理結果
func (scheduler *Scheduler) handleTaskResult(result *common.TaskExecuteResult) {
	var (
		taskLog *common.TaskLog
	)
	delete(scheduler.taskExecutingTable, result.ExecuteInfo.Task.Name)

	// 処理ログを生成する
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		taskLog = &common.TaskLog{
			TaskName: result.ExecuteInfo.Task.Name,
			Command: result.ExecuteInfo.Task.Command,
			Output: string(result.Output),
			PlanTime: result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime: result.StartTime.UnixNano() / 1000 / 1000,
			EndTime: result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			taskLog.Err = result.Err.Error()
		} else {
			taskLog.Err = ""
		}
		Sg_logSink.Append(taskLog)
	}

}

func (scheduler *Scheduler) scheduleLoop() {
	var (
		taskEvent *common.TaskEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		taskResult *common.TaskExecuteResult
	)

	// 一回初期化する(1秒)
	scheduleAfter = scheduler.TrySchedule()

	scheduleTimer = time.NewTimer(scheduleAfter)

	for {
		select {
		case taskEvent = <- scheduler.taskEventChan:
			scheduler.handleTaskEvent(taskEvent)
		case <- scheduleTimer.C:
		case taskResult = <- scheduler.taskResultChan:
			scheduler.handleTaskResult(taskResult)
		}
		scheduleAfter = scheduler.TrySchedule()
		scheduleTimer.Reset(scheduleAfter)
	}
}

// タスク変化イベントをPushする
func (scheduler *Scheduler) PushTaskEvent(taskEvent *common.TaskEvent) {
	scheduler.taskEventChan <- taskEvent
}

// スケジューラを初期化
func InitScheduler() (err error) {
	Sg_scheduler = &Scheduler{
		taskEventChan: make(chan *common.TaskEvent, 1000),
		taskPlanTable: make(map[string]*common.TaskSchedulePlan),
		taskExecutingTable: make(map[string]*common.TaskExecuteInfo),
		taskResultChan: make(chan *common.TaskExecuteResult, 1000),
	}
	// スケジューラ　コルーチンを起動
	go Sg_scheduler.scheduleLoop()
	return
}

func (scheduler *Scheduler) PushTaskResult(taskResult *common.TaskExecuteResult) {
	scheduler.taskResultChan <- taskResult
}