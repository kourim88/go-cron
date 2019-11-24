package master

import (
	"github.com/coreos/etcd/clientv3"
	"time"
	"github.com/gocron/common"
	"encoding/json"
	"context"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type TaskMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	Sg_taskMgr *TaskMgr
)

func InitTaskMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
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

	Sg_taskMgr = &TaskMgr{
		client: client,
		kv: kv,
		lease: lease,
	}
	return
}

// タスク保存
func (taskMgr *TaskMgr) SaveTask(task *common.Task) (oldTask *common.Task, err error) {
	var (
		taskKey string
		taskValue []byte
		putResp *clientv3.PutResponse
		oldTaskObj common.Task
	)

	// etcdのkey
	taskKey = common.TASK_SAVE_DIR + task.Name
	// タスクのjson
	if taskValue, err = json.Marshal(task); err != nil {
		return
	}
	// etcdに保存
	if putResp, err = taskMgr.kv.Put(context.TODO(), taskKey, string(taskValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	// 更新の場合、値を返す
	if putResp.PrevKv != nil {
		// 保存している値を逆シリアル化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldTaskObj); err != nil {
			err = nil
			return
		}
		oldTask = &oldTaskObj
	}
	return
}

// タスク削除
func (taskMgr *TaskMgr) DeleteTask(name string) (oldTask *common.Task, err error) {
	var (
		taskKey string
		delResp *clientv3.DeleteResponse
		oldTaskObj common.Task
	)

	taskKey = common.TASK_SAVE_DIR + name

	if delResp, err = taskMgr.kv.Delete(context.TODO(), taskKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(delResp.PrevKvs) != 0 {
		if err =json.Unmarshal(delResp.PrevKvs[0].Value, &oldTaskObj); err != nil {
			err = nil
			return
		}
		oldTask = &oldTaskObj
	}
	return
}

// タスクリスト
func (taskMgr *TaskMgr) ListTasks() (taskList []*common.Task, err error) {
	var (
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		task *common.Task
	)

	dirKey = common.TASK_SAVE_DIR

	if getResp, err = taskMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	taskList = make([]*common.Task, 0)

	for _, kvPair = range getResp.Kvs {
		task = &common.Task{}
		if err =json.Unmarshal(kvPair.Value, task); err != nil {
			err = nil
			continue
		}
		taskList = append(taskList, task)
	}
	return
}

// タスクを強制的にKill
func (taskMgr *TaskMgr) KillTask(name string) (err error) {
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	killerKey = common.TASK_KILLER_DIR + name

	if leaseGrantResp, err = taskMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = leaseGrantResp.ID

	if _, err = taskMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}