package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/gocron/common"
)

// 分散型ロック(TXNトランザクション)
type TaskLock struct {
	kv clientv3.KV
	lease clientv3.Lease

	taskName string
	cancelFunc context.CancelFunc
	leaseId clientv3.LeaseID
	isLocked bool
}

func InitTaskLock(taskName string, kv clientv3.KV, lease clientv3.Lease) (taskLock *TaskLock) {
	taskLock = &TaskLock{
		kv: kv,
		lease: lease,
		taskName: taskName,
	}
	return
}

func (taskLock *TaskLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <- chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)

	if leaseGrantResp, err = taskLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	// contextはリースをキャンセル用です
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	// リースID
	leaseId = leaseGrantResp.ID

	if keepRespChan, err = taskLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <- keepRespChan:
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	// トランザクションを作る
	txn = taskLock.kv.Txn(context.TODO())

	lockKey = common.TASK_LOCK_DIR + taskLock.taskName

	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	// トランザクションをコミット
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	if !txnResp.Succeeded {	// ロックは占有されている
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	// ロック取得した
	taskLock.leaseId = leaseId
	taskLock.cancelFunc = cancelFunc
	taskLock.isLocked = true
	return

FAIL:
	cancelFunc() // リースの自動更新をキャンセルする
	taskLock.lease.Revoke(context.TODO(), leaseId)
	return
}

func (taskLock *TaskLock) Unlock() {
	if taskLock.isLocked {
		taskLock.cancelFunc()
		taskLock.lease.Revoke(context.TODO(), taskLock.leaseId)
	}
}