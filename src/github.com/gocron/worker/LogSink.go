package worker

import (
	"context"
	"github.com/gocron/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"time"
)

// mongodb
type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.TaskLog
	autoCommitChan chan *common.LogBatch
}

var (
	Sg_logSink *LogSink
)

// バッチでログを保存する
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// ログ保存コルーチン
func (logSink *LogSink) writeLoop() {
	var (
		log *common.TaskLog
		logBatch *common.LogBatch
		commitTimer *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case log = <- logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.AfterFunc(
					time.Duration(Sg_config.TaskLogCommitTimeout) * time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			// logBatch.Logsに新しいログを追加
			logBatch.Logs = append(logBatch.Logs, log)

			if len(logBatch.Logs) >= Sg_config.TaskLogBatchSize {
				logSink.saveLogs(logBatch)
				logBatch = nil
				commitTimer.Stop()
			}
		case timeoutBatch = <- logSink.autoCommitChan:
			if timeoutBatch != logBatch {
				continue
			}
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)

	if client, err = mongo.Connect(
		context.TODO(),
		Sg_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(Sg_config.MongodbConnectTimeout) * time.Millisecond)); err != nil {
		return
	}

	Sg_logSink = &LogSink{
		client: client,
		logCollection: client.Database("gocron").Collection("log"),
		logChan: make(chan *common.TaskLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	go Sg_logSink.writeLoop()
	return
}

// ログ発送
func (logSink *LogSink) Append(taskLog *common.TaskLog) {
	select {
	case logSink.logChan <- taskLog:
	default:
	}
}