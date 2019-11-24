package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/gocron/common"
	"time"
)

type TaskMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	Sg_taskMgr *TaskMgr
)

func (taskMgr *TaskMgr) watchTasks() (err error) {
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		task *common.Task
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		taskName string
		taskEvent *common.TaskEvent
	)

	if getResp, err = taskMgr.kv.Get(
		context.TODO(),
		common.TASK_SAVE_DIR,
		clientv3.WithPrefix()); err != nil {
		return
	}

	// タスクをループ
	for _, kvpair = range getResp.Kvs {
		if task, err = common.UnpackTask(kvpair.Value); err == nil {
			taskEvent = common.BuildTaskEvent(common.TASK_EVENT_SAVE, task)
			Sg_scheduler.PushTaskEvent(taskEvent)
		}
	}

	go func() {
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = taskMgr.watcher.Watch(
			context.TODO(),
			common.TASK_SAVE_DIR,
			clientv3.WithRev(watchStartRevision),
			clientv3.WithPrefix())

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					if task, err = common.UnpackTask(watchEvent.Kv.Value); err != nil {
						continue
					}
					taskEvent = common.BuildTaskEvent(common.TASK_EVENT_SAVE, task)
				case mvccpb.DELETE:
					taskName = common.ExtractTaskName(string(watchEvent.Kv.Key))

					task = &common.Task{Name: taskName}

					taskEvent = common.BuildTaskEvent(common.TASK_EVENT_DELETE, task)
				}
				Sg_scheduler.PushTaskEvent(taskEvent)
			}
		}
	}()
	return
}

func (taskMgr *TaskMgr) watchKiller() {
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		taskEvent *common.TaskEvent
		taskName string
		task *common.Task
	)
	go func() {
		watchChan = taskMgr.watcher.Watch(context.TODO(),
			common.TASK_KILLER_DIR,
			clientv3.WithPrefix())

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					taskName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					task = &common.Task{Name: taskName}
					taskEvent = common.BuildTaskEvent(common.TASK_EVENT_KILL, task)
					Sg_scheduler.PushTaskEvent(taskEvent)
				case mvccpb.DELETE:
				}
			}
		}
	}()
}

func InitTaskMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)

	config = clientv3.Config{
		Endpoints: Sg_config.EtcdEndpoints,
		DialTimeout: time.Duration(Sg_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	Sg_taskMgr = &TaskMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}

	if err = Sg_taskMgr.watchTasks(); err != nil {
		return
	}

	Sg_taskMgr.watchKiller()

	return
}

func (taskMgr *TaskMgr) CreateTaskLock(taskName string) (taskLock *TaskLock){
	taskLock = InitTaskLock(taskName, taskMgr.kv, taskMgr.lease)
	return
}