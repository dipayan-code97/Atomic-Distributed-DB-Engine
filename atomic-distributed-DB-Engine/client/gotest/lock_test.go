package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestLock_LockAndUnLock(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestLockUnLock"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock unlock Fail %v", err)
			return
		}

		relock := client.lock(client.testString2Key("TestLockUnLock"), 5, 5)
		err = relock.lock()
		if err != nil {
			t.Errorf("lock ReLock Fail %v", err)
			return
		}

		err = relock.unlock()
		if err != nil {
			t.Errorf("lock ReUnlock Fail %v", err)
			return
		}
	})
}

func TestLock_LockUpdate(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestLockUpdate"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		updateLock := client.lock(client.testString2Key("TestLockUpdate"), 5, 5)
		updateLock.lockId = lock.lockId
		err = updateLock.LockUpdate()
		if err.CommandResult.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("lock lock Update Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock unlock Fail %v", err)
			return
		}
	})
}

func TestLock_LockShow(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestLockShow"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		showLock := client.lock(client.testString2Key("TestLockShow"), 5, 5)
		err = showLock.LockShow()
		if err.CommandResult.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("lock Show Fail %v", err)
			return
		}

		if err.CommandResult.LockId != lock.lockId {
			t.Errorf("lock Show LockId error %x %x", err.CommandResult.LockId, lock.lockId)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock unlock Fail %v", err)
			return
		}
	})
}

func TestLock_UnLockHead(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestUnLockHead"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		checkLock := client.lock(client.testString2Key("TestUnLockHead"), 0, 5)
		err = checkLock.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("lock Check lock Fail %v", err)
			return
		}

		unlockHeadLock := client.lock(client.testString2Key("TestUnLockHead"), 5, 5)
		err = unlockHeadLock.UnlockHead()
		if err.CommandResult.Result != 0 {
			t.Errorf("lock unlock Fail %v", err)
			return
		}

		relock := client.lock(client.testString2Key("TestUnLockHead"), 5, 5)
		err = relock.lock()
		if err != nil {
			t.Errorf("lock ReLock Fail %v", err)
			return
		}

		err = relock.unlock()
		if err != nil {
			t.Errorf("lock ReUnlock Fail %v", err)
			return
		}
	})
}

func TestLock_CancelWait(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestCancelWait"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		waitLock := client.lock(client.testString2Key("TestCancelWait"), 5, 0)
		go func() {
			err = waitLock.lock()
			if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
				t.Errorf("lock wait Cancel lock Fail %v", err)
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)

		err = waitLock.cancelWait()
		if err.CommandResult.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("lock Cancel Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock unlock Fail %v", err)
			return
		}
		time.Sleep(20 * time.Millisecond)
	})
}

func TestLock_LockCount(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock1 := client.lock(client.testString2Key("TestLockCount"), 0, 5)
		lock1.SetCount(2)
		err := lock1.lock()
		if err != nil {
			t.Errorf("lock Lock1 Fail %v", err)
			return
		}

		lock2 := client.lock(client.testString2Key("TestLockCount"), 0, 5)
		lock2.SetCount(2)
		err = lock2.lock()
		if err != nil {
			t.Errorf("lock Lock2 Fail %v", err)
			return
		}

		lock3 := client.lock(client.testString2Key("TestLockCount"), 0, 5)
		lock3.SetCount(2)
		err = lock3.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("lock Lock3 Fail %v", err)
			return
		}

		showLock := client.lock(client.testString2Key("TestLockCount"), 5, 5)
		err = showLock.LockShow()
		if err.CommandResult.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("lock Show Fail %v", err)
			return
		}
		if err.CommandResult.Lcount != 2 {
			t.Errorf("lock Count Fail %v", err.CommandResult.Lcount)
			return
		}

		err = lock1.unlock()
		if err != nil {
			t.Errorf("lock Unlock1 Fail %v", err)
			return
		}

		err = lock2.unlock()
		if err != nil {
			t.Errorf("lock Unlock2 Fail %v", err)
			return
		}

		err = lock3.unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("lock Unlock3 Fail %v", err)
			return
		}
	})
}

func TestLock_LockRCount(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestLockRCount"), 0, 5)
		lock.setRcount(2)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		err = lock.lock()
		if err != nil {
			t.Errorf("lock ReLock Fail %v", err)
			return
		}

		err = lock.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("lock Check lock Fail %v", err)
			return
		}

		showLock := client.lock(client.testString2Key("TestLockRCount"), 5, 5)
		err = showLock.LockShow()
		if err.CommandResult.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("lock Show Fail %v", err)
			return
		}
		if err.CommandResult.Lrcount != 2 {
			t.Errorf("lock Count Fail %v", err.CommandResult.Lrcount)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock unlock Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock ReUnlock Fail %v", err)
			return
		}

		err = lock.unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("lock Check unlock Fail %v", err)
			return
		}

		lock = client.lock(client.testString2Key("TestLockRCount"), 0, 5)
		lock.setRcount(2)
		err = lock.lock()
		if err != nil {
			t.Errorf("lock Check All lock Fail %v", err)
			return
		}

		err = lock.lock()
		if err != nil {
			t.Errorf("lock Check All ReLock Fail %v", err)
			return
		}

		lock.setRcount(0)
		err = lock.unlock()
		if err != nil {
			t.Errorf("lock Check All unlock Fail %v", err)
			return
		}

		err = lock.unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("lock Check All Check unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ZeroTimeout(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestZeroTimeout"), 0, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		err = lock.lock()
		if err == nil || err.CommandResult.Result == protocol.RESULT_TIMEOUT {
			t.Errorf("lock Check lock Timeout Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("lock unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ZeroExpried(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestZeroExpried"), 0, 0)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		err = lock.lock()
		if err != nil {
			t.Errorf("lock ReLock Fail %v", err)
			return
		}
	})
}

func TestLock_UnlockWait(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestUnlockWait"), 0, 5)
		lock.setTimeoutFlag(protocol.TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK)
		err := lock.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("lock lock Fail %v", err)
			return
		}
	})
}

func TestLock_TimeoutTeverseKeyLock(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestTimeoutRKL"), 10, 5)
		lock.setTimeoutFlag(protocol.TIMEOUT_FLAG_MILLISECOND_TIME | protocol.TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK | protocol.TIMEOUT_FLAG_REVERSE_KEY_LOCK_WHEN_TIMEOUT)
		err := lock.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		lockKey := [16]byte{}
		for i := 0; i < 16; i++ {
			lockKey[i] = lock.lockKey[15-i]
		}
		lock = client.lock(lockKey, 0, 5)
		time.Sleep(20 * time.Millisecond)
		err = lock.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("lock Check lock Fail %v", err)
			return
		}

		err = lock.UnlockHead()
		if err.CommandResult.Result != 0 {
			t.Errorf("lock unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ExpriedReverseKeyLock(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.lock(client.testString2Key("TestExpriedRKL"), 50, 10)
		lock.setExpiredFlag(protocol.EXPRIED_FLAG_MILLISECOND_TIME | protocol.EXPRIED_FLAG_REVERSE_KEY_LOCK_WHEN_EXPRIED)
		err := lock.lock()
		if err != nil {
			t.Errorf("lock lock Fail %v", err)
			return
		}

		lockKey := [16]byte{}
		for i := 0; i < 16; i++ {
			lockKey[i] = lock.lockKey[15-i]
		}
		lock = client.lock(lockKey, 0, 5)
		time.Sleep(20 * time.Millisecond)
		err = lock.lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("lock Check lock Fail %v", err)
			return
		}

		err = lock.UnlockHead()
		if err.CommandResult.Result != 0 {
			t.Errorf("lock unlock Fail %v", err)
			return
		}
	})
}
