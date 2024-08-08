package adapter

import (
	"fmt"
	"github.com/snower/slock/client/godev"
	"math/rand"
	"sync/atomic"
	"time"
)

func runClientBenchmark(slockClient *godev.Client, count *uint32, maxCount uint32, keys [][16]byte, waiter chan bool, timeout uint32, expried uint32) {
	var lockKey [16]byte
	for {
		if keys == nil {
			lockKey = slockClient.selectDB(0).generateLockID()
		} else {
			lockKey = keys[rand.Intn(len(keys))]
		}
		lock := slockClient.lock(lockKey, timeout, expried)

		err := lock.lock()
		if err != nil {
			fmt.Printf("lock error %v\n", err)
			continue
		}

		err = lock.unlock()
		if err != nil {
			fmt.Printf("UnLock error %v\n", err)
			continue
		}

		atomic.AddUint32(count, 2)
		if *count > maxCount {
			close(waiter)
			return
		}
	}
}

func StartClientBenchmark(clientCount int, concurrentc int, maxCount int, keys [][16]byte, port int, host string, timeout uint32, expried uint32) {
	fmt.Printf("Run %d Client, %d concurrentc, %d Count lock and unlock\n", clientCount, concurrentc, maxCount)

	clients := make([]*godev.Client, clientCount)
	waiters := make([]chan bool, concurrentc)
	defer func() {
		for _, c := range clients {
			if c == nil {
				continue
			}
			_ = c.close()
		}
	}()

	for i := 0; i < clientCount; i++ {
		c := godev.BuildNewClient(host, uint(port))
		err := c.open()
		if err != nil {
			fmt.Printf("Connect error: %v", err)
			return
		}
		clients[i] = c
	}
	fmt.Printf("Client Opened %d\n", len(clients))

	var count uint32
	startTime := time.Now().UnixNano()
	for i := 0; i < concurrentc; i++ {
		waiters[i] = make(chan bool, 1)
		go runClientBenchmark(clients[i%clientCount], &count, uint32(maxCount), keys, waiters[i], timeout, expried)
	}
	for _, waiter := range waiters {
		<-waiter
	}
	endTime := time.Now().UnixNano()
	pt := float64(endTime-startTime) / 1000000000.0
	fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count)/pt)
}
