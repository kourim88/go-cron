package master

import (
	"github.com/coreos/etcd/clientv3"
	"time"
	"context"
	"github.com/gocron/common"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	Sg_workerMgr *WorkerMgr
)

// 正常なworker Node
func (workerMgr *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	var (
		getResp *clientv3.GetResponse
		kv *mvccpb.KeyValue
		workerIP string
	)

	workerArr = make([]string, 0)

	if getResp, err = workerMgr.kv.Get(context.TODO(), common.TASK_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kv = range getResp.Kvs {
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}

func InitWorkerMgr() (err error) {
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

	Sg_workerMgr = &WorkerMgr{
		client :client,
		kv: kv,
		lease: lease,
	}
	return
}