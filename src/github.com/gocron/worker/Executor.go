package worker

import (
	"github.com/gocron/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {

}

var (
	Sg_executor *Executor
)

func (executor *Executor) ExecuteTask(info *common.TaskExecuteInfo) {
	go func() {
		var (
			cmd *exec.Cmd
			err error
			output []byte
			result *common.TaskExecuteResult
			taskLock *TaskLock
		)

		result = &common.TaskExecuteResult{
			ExecuteInfo: info,
			Output: make([]byte, 0),
		}

		taskLock = Sg_taskMgr.CreateTaskLock(info.Task.Name)

		result.StartTime = time.Now()

		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		err = taskLock.TryLock()
		defer taskLock.Unlock()

		if err != nil { // ロック失敗した場合
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// ロック正常
			result.StartTime = time.Now()

			// Shellコマンドを実行
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Task.Command)

			// Outputを所得
			output, err = cmd.CombinedOutput()

			// タスク終了時間を記録する
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		Sg_scheduler.PushTaskResult(result)
	}()
}

func InitExecutor() (err error) {
	Sg_executor = &Executor{}
	return
}