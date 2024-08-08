package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"testing"
)

func TestRWLock_LockAndUnLock(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.readerWriterLock(client.testString2Key("TestRWLock"), 0, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("rwLock lock Fail %v", err)
			return
		}

		err = lock.rLock()
		if err == nil {
			t.Errorf("rwLock rLock Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("rwLock UnLock Fail %v", err)
			return
		}

		err = lock.rLock()
		if err != nil {
			t.Errorf("rwLock rLock Fail %v", err)
			return
		}

		err = lock.lock()
		if err == nil {
			t.Errorf("rwLock lock Fail %v", err)
			return
		}

		err = lock.rUnlock()
		if err != nil {
			t.Errorf("rwLock RUnLock Fail %v", err)
			return
		}
	})
}
