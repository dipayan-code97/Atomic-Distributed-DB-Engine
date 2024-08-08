package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"sync"
	"testing"
	"time"
)

func TestGroupEvent_Default(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		event := client.groupEvent(client.testString2Key("TestGroupEvent"), 1, 1, 5, 5)
		isSeted, err := event.isSet()
		if err != nil {
			t.Errorf("event Check Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("event Check Seted Status error %v", err)
			return
		}

		err = event.clear()
		if err != nil {
			t.Errorf("event clear Fail %v", err)
			return
		}

		isSeted, err = event.isSet()
		if err != nil {
			t.Errorf("event clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("event clear Seted Status error %v", err)
			return
		}

		err = event.set()
		if err != nil {
			t.Errorf("event set Fail %v", err)
			return
		}

		isSeted, err = event.isSet()
		if err != nil {
			t.Errorf("event set Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("event set Seted Status error %v", err)
			return
		}

		err = event.clear()
		if err != nil {
			t.Errorf("event wait clear Fail %v", err)
			return
		}

		go func() {
			time.Sleep(20 * time.Millisecond)
			err = event.set()
			if err != nil {
				t.Errorf("event wakeup set Fail %v", err)
				return
			}
		}()

		succed, err := event.wait(60)
		if err != nil {
			t.Errorf("event wait Fail %v", err)
			return
		}
		if !succed {
			t.Errorf("event wait error %v", succed)
			return
		}
	})
}

func TestGroupEvent_Wakeup(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		event := client.groupEvent(client.testString2Key("TestGroupWakeup"), 1, 1, 5, 5)
		err := event.clear()
		if err != nil {
			t.Errorf("event wait clear Fail %v", err)
			return
		}

		isSeted, err := event.isSet()
		if err != nil {
			t.Errorf("event clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("event clear Seted Status error %v", isSeted)
			return
		}

		succedCount := 0
		glock := sync.Mutex{}
		for i := 0; i < 4; i++ {
			go func(i int) {
				defer func() {
					glock.Lock()
					succedCount += 1
					glock.Unlock()
				}()
				event := client.groupEvent(client.testString2Key("TestGroupWakeup"), uint64(i+2), 1, 5, 5)
				succed, err := event.wait(60)
				if err != nil {
					t.Errorf("event wait Fail %v", err)
					return
				}
				if !succed {
					t.Errorf("event wait error %v", succed)
					return
				}
			}(i)
		}

		time.Sleep(20 * time.Millisecond)
		if succedCount != 0 {
			t.Errorf("event Group wait Fail %v", err)
			return
		}
		err = event.wakeup()
		if err != nil {
			t.Errorf("event wakeup set Fail %v", err)
			return
		}
		time.Sleep(time.Second)
		if succedCount != 4 {
			t.Errorf("event wakeup Succed Count Fail %v", err)
			return
		}

		isSeted, err = event.isSet()
		if err != nil {
			t.Errorf("event wakeup Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("event wakeup Seted Status error %v", isSeted)
			return
		}

		event = client.groupEvent(client.testString2Key("TestGroupWakeup"), 1, 1, 5, 5)
		succed, err := event.wait(1)
		if err != nil {
			t.Errorf("event wait Less Version Fail %v", err)
			return
		}
		if !succed {
			t.Errorf("event wait Less Version error %v", succed)
			return
		}

		err = event.set()
		if err != nil {
			t.Errorf("event set Fail %v", err)
			return
		}
	})
}
