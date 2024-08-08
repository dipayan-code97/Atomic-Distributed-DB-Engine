package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"testing"
	"time"
)

func TestEvent_DefaultSet(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		event := client.event(client.testString2Key("TestDefaultSet"), 5, 5, true)
		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("event Check Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("event Check Seted Status error %v", err)
			return
		}

		err = event.Clear()
		if err != nil {
			t.Errorf("event clear Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("event clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("event clear Seted Status error %v", err)
			return
		}

		err = event.Set()
		if err != nil {
			t.Errorf("event set Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("event set Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("event set Seted Status error %v", err)
			return
		}

		err = event.Clear()
		if err != nil {
			t.Errorf("event wait clear Fail %v", err)
			return
		}

		go func() {
			time.Sleep(20 * time.Millisecond)
			err = event.Set()
			if err != nil {
				t.Errorf("event wakeup set Fail %v", err)
				return
			}
		}()

		succed, err := event.Wait(60)
		if err != nil {
			t.Errorf("event wait Fail %v", err)
			return
		}
		if !succed {
			t.Errorf("event wait error %v", err)
			return
		}
	})
}

func TestEvent_DefaultClear(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		event := client.event(client.testString2Key("TestDefaultClear"), 5, 5, false)
		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("event Check Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("event Seted Status error %v", err)
			return
		}

		err = event.Set()
		if err != nil {
			t.Errorf("event set Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("event set Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("event set Seted Status error %v", err)
			return
		}

		err = event.Clear()
		if err != nil {
			t.Errorf("event clear Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("event clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("event clear Seted Status error %v", err)
			return
		}

		go func() {
			time.Sleep(20 * time.Millisecond)
			err = event.Set()
			if err != nil {
				t.Errorf("event wakeup set Fail %v", err)
				return
			}
		}()

		succed, err := event.Wait(60)
		if err != nil {
			t.Errorf("event wait Fail %v", err)
			return
		}
		if !succed {
			t.Errorf("event wait error %v", err)
			return
		}

		err = event.Clear()
		if err != nil {
			t.Errorf("event clear Fail %v", err)
			return
		}
	})
}
