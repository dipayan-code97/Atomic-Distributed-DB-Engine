package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"github.com/snower/slock/protocol/protobuf"
	"testing"
	"time"
)

func TestDB_ListLocks(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestListLocks"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("DB lock Fail %v", err)
			return
		}

		result, lerr := client.selectDB(0).listLocks(5)
		if lerr != nil {
			t.Errorf("DB listLocks Fail %v", err)
			return
		}
		if len(result.Locks) == 0 {
			t.Errorf("DB Count error %v", result)
			return
		}

		var listLock *protobuf.LockDBLock = nil
		for _, l := range result.Locks {
			lockKey := [16]byte{}
			copy(lockKey[:], l.LockKey)
			if lock.lockKey == lockKey {
				listLock = l
			}
		}
		if listLock == nil || listLock.LockedCount != 1 {
			if len(result.Locks) == 0 {
				t.Errorf("DB Not Find error %v", result)
				return
			}
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("DB unlock Fail %v", err)
			return
		}
	})
}

func TestDB_ListLockeds(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestListLockeds"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("DB lock Fail %v", err)
			return
		}

		result, lerr := client.selectDB(0).LockLockeds(lock.lockKey, 5)
		if lerr != nil {
			t.Errorf("DB LockLockeds Fail %v", err)
			return
		}
		if len(result.Locks) != 1 {
			t.Errorf("DB Count error %v", result)
			return
		}

		lockId := [16]byte{}
		copy(lockId[:], result.Locks[0].LockId)
		if lockId != lock.lockId {
			t.Errorf("DB Find error %v", result)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("DB unlock Fail %v", err)
			return
		}
	})
}

func TestDB_ListWaits(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestListWaits"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("DB lock Fail %v", err)
			return
		}

		waitLock := client.lock(client.testString2Key("TestListWaits"), 5, 0)
		go func() {
			err := waitLock.lock()
			if err != nil {
				t.Errorf("DB wait lock Fail %v", err)
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)

		result, lerr := client.selectDB(0).listLockWaits(lock.lockKey, 5)
		if lerr != nil {
			t.Errorf("DB listLockWaits Fail %v", err)
			return
		}
		if len(result.Locks) != 1 {
			t.Errorf("DB Count error %d %v", len(result.Locks), result)
			return
		}

		lockId := [16]byte{}
		copy(lockId[:], result.Locks[0].LockId)
		if lockId != waitLock.lockId {
			t.Errorf("DB Find error %v", result)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("DB unlock Fail %v", err)
			return
		}
		time.Sleep(20 * time.Millisecond)
	})
}
