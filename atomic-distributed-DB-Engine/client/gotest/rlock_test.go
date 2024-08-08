package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"github.com/snower/slock/protocol"
	"testing"
)

func TestRLock_LockAndUnLock(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		lock := client.rLock(client.testString2Key("TestRLock"), 5, 5)
		err := lock.lock()
		if err != nil {
			t.Errorf("rLock Lock1 Fail %v", err)
			return
		}

		err = lock.lock()
		if err != nil {
			t.Errorf("rLock Lock2 Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("rLock UnLock1 Fail %v", err)
			return
		}

		err = lock.unlock()
		if err != nil {
			t.Errorf("rLock UnLock2 Fail %v", err)
			return
		}

		err = lock.unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("rLock UnLock Fail %v", err)
			return
		}
	})
}
