package gotest

import (
	"github.com/snower/slock/client/godev"
	"testing"
)

func checkChildTreeLock(t *testing.T, client *godev.Client, rootLock *godev.TreeLock, childLock *godev.TreeLock, lock *godev.TreeLeafLock, depth int) {
	clock1 := childLock.NewLeafLock()
	err := clock1.Lock()
	if err != nil {
		t.Errorf("treeLock Child Lock1 Fail %v", err)
		return
	}
	clock2 := childLock.NewLeafLock()
	err = clock2.Lock()
	if err != nil {
		t.Errorf("treeLock Child Lock2 Fail %v", err)
		return
	}

	testLock := client.lock(rootLock.GetLockKey(), 0, 0)
	err = testLock.lock()
	if err == nil {
		t.Errorf("treeLock Test childLock Locked Root lock Fail %v", err)
		return
	}
	testLock = client.lock(childLock.GetLockKey(), 0, 0)
	err = testLock.lock()
	if err == nil {
		t.Errorf("treeLock Test childLock Locked Child lock Fail %v", err)
		return
	}

	err = lock.Unlock()
	if err != nil {
		t.Errorf("treeLock Root UnLock Fail %v", err)
		return
	}

	testLock = client.lock(rootLock.GetLockKey(), 0, 0)
	err = testLock.lock()
	if err == nil {
		t.Errorf("treeLock Test Root Unlocked Root lock Fail %v", err)
		return
	}
	testLock = client.lock(childLock.GetLockKey(), 0, 0)
	err = testLock.lock()
	if err == nil {
		t.Errorf("treeLock Test Root Unlocked childLock Locked Child lock Fail %v", err)
		return
	}

	if depth-1 > 0 {
		_ = lock.Lock()
		checkChildTreeLock(t, client, childLock, childLock.NewChild(), lock, depth-1)
		_ = lock.Lock()
		checkChildTreeLock(t, client, childLock, childLock.NewChild(), lock, depth-1)
	}

	err = clock1.Unlock()
	if err != nil {
		t.Errorf("treeLock Child UnLock1 Fail %v", err)
		return
	}

	testLock = client.lock(rootLock.GetLockKey(), 0, 0)
	err = testLock.lock()
	if err == nil {
		t.Errorf("treeLock Test childLock Unlocked Root lock Fail %v", err)
		return
	}
	testLock = client.lock(childLock.GetLockKey(), 0, 0)
	err = testLock.lock()
	if err == nil {
		t.Errorf("treeLock Test childLock Unlocked childLock Locked Child lock Fail %v", err)
		return
	}

	err = clock2.Unlock()
	if err != nil {
		t.Errorf("treeLock Child UnLock2 Fail %v", err)
		return
	}

	testLock = client.lock(childLock.GetLockKey(), 1, 0)
	err = testLock.lock()
	if err != nil {
		t.Errorf("treeLock Test Child lock Fail %v", err)
		return
	}
}

func TestTreeLock(t *testing.T) {
	testWithClient(t, func(client *godev.Client) {
		rootLock := client.treeLock(testString2Key("TestTreeLock"), godev.RootKey, 5, 10)

		err := rootLock.Lock()
		if err != nil {
			t.Errorf("treeLock lock Fail %v", err)
			return
		}
		err = rootLock.Unlock()
		if err != nil {
			t.Errorf("treeLock UnLock Fail %v", err)
			return
		}
		err = rootLock.Wait(10)
		if err != nil {
			t.Errorf("treeLock wait Fail %v", err)
			return
		}

		lock := rootLock.NewLeafLock()
		err = lock.Lock()
		if err != nil {
			t.Errorf("treeLock Root lock Fail %v", err)
			return
		}

		checkChildTreeLock(t, client, rootLock, rootLock.NewChild(), lock, 5)

		err = rootLock.Wait(10)
		if err != nil {
			t.Errorf("treeLock wait Fail %v", err)
			return
		}

		testLock := client.lock(rootLock.GetLockKey(), 1, 0)
		err = testLock.lock()
		if err != nil {
			t.Errorf("treeLock Test Root lock Fail %v", err)
			return
		}
	})
}
