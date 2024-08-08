package gotest

import (
	"github.com/snower/slock/client/godev"
	"sync"
	"testing"
)

var testClient *godev.Client = nil
var testGlock sync.Mutex

func testString2Key(key string) [16]byte {
	bkey := [16]byte{}
	klen := 16
	if len(key) < 16 {
		klen = len(key)
	}

	for i := 0; i < klen; i++ {
		bkey[i] = key[i]
	}
	return bkey
}

func testWithClient(t *testing.T, doTestFunc func(client *godev.Client)) {
	testGlock.Lock()
	client := testClient
	testGlock.Unlock()

	if client == nil || client.protocol == nil {
		client = godev.BuildNewClient("127.0.0.1", 5658)
		err := client.open()
		if err != nil {
			t.Errorf("Client open Fail %v", err)
			return
		}
	}
	doTestFunc(client)

	testGlock.Lock()
	if testClient != nil {
		testGlock.Unlock()
		_ = client.close()
		return
	}
	testClient = client
	testGlock.Unlock()
}

func TestClient_Open(t *testing.T) {
	client := godev.BuildNewClient("127.0.0.1", 5658)
	err := client.open()
	if err != nil {
		t.Errorf("Client open Fail %v", err)
		return
	}

	lock := client.lock(testString2Key("testClient"), 5, 5)
	lerr := lock.lock()
	if lerr != nil {
		t.Errorf("Client lock Fail %v", lerr)
		return
	}

	ulerr := lock.unlock()
	if ulerr != nil {
		t.Errorf("Client UnLock Fail %v", ulerr)
		return
	}

	err = client.close()
	if err != nil {
		t.Errorf("Client close Fail %v", err)
		return
	}
}

func TestReplsetClient_Open(t *testing.T) {
	client := godev.NewReplsetClient([]string{"127.0.0.1:5658"})
	err := client.open()
	if err != nil {
		t.Errorf("Client open Fail %v", err)
		return
	}

	lock := client.lock(testString2Key("testReplset"), 5, 5)
	lerr := lock.lock()
	if lerr != nil {
		t.Errorf("Client lock Fail %v", lerr)
		return
	}

	ulerr := lock.unlock()
	if ulerr != nil {
		t.Errorf("Client UnLock Fail %v", ulerr)
		return
	}

	err = client.close()
	if err != nil {
		t.Errorf("Client close Fail %v", err)
		return
	}
}
