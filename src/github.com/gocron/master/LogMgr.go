package master

import (
	"github.com/mongodb/mongo-go-driver/mongo"
	"context"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"time"
	"github.com/gocron/common"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
)

// mongodbログ管理
type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	Sg_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	if client, err = mongo.Connect(
		context.TODO(),
		Sg_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(Sg_config.MongodbConnectTimeout) * time.Millisecond)); err != nil {
		return
	}

	Sg_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database("gocron").Collection("log"),
	}
	return
}

// タスクログリスト
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.TaskLog, err error){
	var (
		filter *common.TaskLogFilter
		logSort *common.SortLogByStartTime
		cursor mongo.Cursor
		taskLog *common.TaskLog
	)

	logArr = make([]*common.TaskLog, 0)

	// フィルター条件
	filter = &common.TaskLogFilter{TaskName: name}

	logSort = &common.SortLogByStartTime{SortOrder: -1}

	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findopt.Sort(logSort), findopt.Skip(int64(skip)), findopt.Limit(int64(limit))); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		taskLog = &common.TaskLog{}

		if err = cursor.Decode(taskLog); err != nil {
			continue
		}

		logArr = append(logArr, taskLog)
	}
	return
}