package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"testing"
)

func TestSemaphore_Acquire(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		semaphore := client.semaphore(client.testString2Key("TestRWLock"), 0, 5, 2)
		err := semaphore.Acquire()
		if err != nil {
			t.Errorf("semaphore Acquire1 Fail %v", err)
			return
		}

		err = semaphore.Acquire()
		if err != nil {
			t.Errorf("semaphore Acquire2 Fail %v", err)
			return
		}

		count, err := semaphore.Count()
		if err != nil {
			t.Errorf("semaphore Count Fail %v", err)
			return
		}
		if count != 2 {
			t.Errorf("semaphore Count error %v", count)
			return
		}

		err = semaphore.Acquire()
		if err == nil {
			t.Errorf("semaphore Acquire3 Fail %v", err)
			return
		}

		err = semaphore.Release()
		if err != nil {
			t.Errorf("semaphore Release1 Fail %v", err)
			return
		}

		count, err = semaphore.Count()
		if err != nil {
			t.Errorf("semaphore ReCount Fail %v", err)
			return
		}
		if count != 1 {
			t.Errorf("semaphore ReCount error %v", count)
			return
		}

		err = semaphore.Acquire()
		if err != nil {
			t.Errorf("semaphore ReAcquire3 Fail %v", err)
			return
		}

		err = semaphore.ReleaseAll()
		if err != nil {
			t.Errorf("semaphore ReleaseAll Fail %v", err)
			return
		}

		count, err = semaphore.Count()
		if err != nil {
			t.Errorf("semaphore clear Count Fail %v", err)
			return
		}
		if count != 0 {
			t.Errorf("semaphore clear Count error %v", count)
			return
		}
	})
}
