package gotest

import (
	"github.com/snower/slock/client"
	"github.com/snower/slock/client/godev"
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestFlow_MaxConcurrentFlow(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		flow := client.maxConcurrentFlow(client.testString2Key("TestMaxConcFlow"), 1, 5, 5)
		err := flow.acquire()
		if err != nil {
			t.Errorf("maxConcurrentFlow acquire Fail %v", err)
			return
		}

		checkFlow := client.maxConcurrentFlow(client.testString2Key("TestMaxConcFlow"), 1, 0, 5)
		err = checkFlow.acquire()
		if err == nil || err.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("maxConcurrentFlow Check acquire Fail %v", err)
			return
		}

		err = flow.release()
		if err != nil {
			t.Errorf("maxConcurrentFlow release Fail %v", err)
			return
		}

		recheckFlow := client.maxConcurrentFlow(client.testString2Key("TestMaxConcFlow"), 1, 5, 0)
		err = recheckFlow.acquire()
		if err != nil {
			t.Errorf("maxConcurrentFlow Recheck acquire Fail %v", err)
			return
		}
	})
}

func TestFlow_TokenBucketFlow(t *testing.T) {
	client.testWithClient(t, func(client *godev.Client) {
		flow := client.tokenBucketFlow(client.testString2Key("TestTokBucFlow"), 1, 5, 0.1)
		err := flow.acquire()
		if err != nil {
			t.Errorf("maxConcurrentFlow acquire Fail %v", err)
			return
		}

		checkFlow := client.tokenBucketFlow(client.testString2Key("TestTokBucFlow"), 1, 0, 0.1)
		err = checkFlow.acquire()
		if err == nil || err.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("maxConcurrentFlow Check acquire Fail %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond)
		recheckFlow := client.tokenBucketFlow(client.testString2Key("TestTokBucFlow"), 1, 5, 0.1)
		err = recheckFlow.acquire()
		if err != nil {
			t.Errorf("maxConcurrentFlow Recheck acquire Fail %v", err)
			return
		}
	})
}
