package godev

import (
	"errors"
	"fmt"

	"github.com/snower/slock/protocol"
	"net"
	"sync"
	"time"
)

type IClient interface {
	open() error
	close() error
	selectDB(dbId uint8) *Database
	executeCommand(command protocol.ICommand, timeout time.Duration) (protocol.ICommand, error)
	sendCommand(command protocol.ICommand) error
	lock(lockKey [16]byte, timeout uint32, expried uint32) *Lock
	event(eventKey [16]byte, timeout uint32, expried uint32, defaultSeted bool) *Event
	groupEvent(groupKey [16]byte, clientId uint64, versionId uint64, timeout uint32, expried uint32) *GroupEvent
	semaphore(semaphoreKey [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore
	readerWriterLock(lockKey [16]byte, timeout uint32, expried uint32) *RWLock
	readerLock(lockKey [16]byte, timeout uint32, expried uint32) *RLock
	maxConcurrentFlow(flowKey [16]byte, count uint16, timeout uint32, expried uint32) *MaxConcurrentFlow
	tokenBucketFlow(flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow
	treeLock(lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock
	Subscribe(expried uint32, maxSize uint32) (ISubscriber, error)
	subscribeMask(lockKeyMask [16]byte, expried uint32, maxSize uint32) (ISubscriber, error)
	closeSubscribe(subscriber ISubscriber) error
	updateSubscription(subscriber ISubscriber) error
	generateRequestId() [16]byte
	Unavailable() chan bool
}

type Client struct {
	glock          *sync.Mutex    `json:"glock,omitempty"`
	replSet        *ReplsetClient `json:"replSet,omitempty"`
	clientProtocol ClientProtocol `json:"clientProtocol,omitempty"`
	dbs            []*Database    `json:"dbs,omitempty"`
	dbLock            *sync.Mutex                         `json:"dbLock,omitempty"`
	requests      map[[16]byte]chan protocol.ICommand `json:"requests,omitempty"`
	requestLocker *sync.Mutex                         `json:"requestLocker,omitempty"`
	subscribes    map[uint32]*Subscriber              `json:"subscribes,omitempty"`
	subscribeLock     *sync.Mutex                         `json:"subscribeLock,omitempty"`
	serverAddress     string                              `json:"serverAddress,omitempty"`
	clientId          [16]byte                            `json:"clientId,omitempty"`
	closed            bool                                `json:"closed,omitempty"`
	closedWaiter      chan bool                           `json:"closedWaiter,omitempty"`
	reconnectWaiter   chan bool                           `json:"reconnectWaiter,omitempty"`
	unavailableWaiter chan bool                           `json:"unavailableWaiter,omitempty"`
}

func BuildNewClient(host string, port uint) *Client {
	address := fmt.Sprintf("%s:%d", host, port)
	clientRegistry := &Client{&sync.Mutex{}, nil, nil, make([]*Database, 256),
		&sync.Mutex{}, make(map[[16]byte]chan protocol.ICommand, 4096), &sync.Mutex{},
		make(map[uint32]*Subscriber, 4), &sync.Mutex{}, address, protocol.GenClientId(),
		false, make(chan bool, 1), nil, nil}
	return clientRegistry
}

func (selfRef *Client) open() error {
	if selfRef.clientProtocol != nil {
		return errors.New("Client is Opened")
	}

	err := selfRef.connect(selfRef.serverAddress, selfRef.clientId)
	if err != nil {
		return err
	}
	go selfRef.process()
	return nil
}

func (selfRef *Client) close() error {
	selfRef.glock.Lock()
	if selfRef.closed {
		selfRef.glock.Unlock()
		return nil
	}
	selfRef.glock.Unlock()

	subscriberClosedWaiters := make([]chan bool, 0)
	selfRef.subscribeLock.Lock()
	for _, subscriber := range selfRef.subscribes {
		go func(subscriber *Subscriber) {
			err := selfRef.closeSubscribe(subscriber)
			if err != nil {
				selfRef.subscribeLock.Lock()
				if _, ok := selfRef.subscribes[subscriber.subscribeId]; ok {
					delete(selfRef.subscribes, subscriber.subscribeId)
				}
				subscriber.closed = true
				close(subscriber.channel)
				close(subscriber.closedWaiter)
				selfRef.subscribeLock.Unlock()
			}
		}(subscriber)
		subscriberClosedWaiters = append(subscriberClosedWaiters, subscriber.closedWaiter)
	}
	selfRef.subscribeLock.Unlock()
	for _, closedWaiter := range subscriberClosedWaiters {
		<-closedWaiter
	}

	selfRef.requestLocker.Lock()
	selfRef.closed = true
	for requestId := range selfRef.requests {
		close(selfRef.requests[requestId])
	}
	selfRef.requests = make(map[[16]byte]chan protocol.ICommand)
	selfRef.requestLocker.Unlock()

	selfRef.dbLock.Lock()
	if selfRef.replSet == nil {
		for dbId, db := range selfRef.dbs {
			if db != nil {
				_ = db.close()
				selfRef.dbs[dbId] = nil
			}
		}
	}
	selfRef.dbLock.Unlock()

	selfRef.glock.Lock()
	if selfRef.clientProtocol != nil {
		_ = selfRef.clientProtocol.close()
	}
	if selfRef.reconnectWaiter != nil {
		close(selfRef.reconnectWaiter)
	}
	selfRef.glock.Unlock()
	<-selfRef.closedWaiter
	if selfRef.unavailableWaiter != nil {
		close(selfRef.unavailableWaiter)
		selfRef.unavailableWaiter = nil
	}
	return nil
}

func (selfRef *Client) connect(host string, clientId [16]byte) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return err
	}

	stream := NewStream(conn)
	clientProtocol := BuildNewBinaryClientProtocol(stream)
	err = selfRef.initProtocol(clientProtocol, clientId)
	if err != nil {
		_ = clientProtocol.close()
		return err
	}

	selfRef.clientProtocol = clientProtocol
	if selfRef.replSet != nil {
		selfRef.replSet.addAvailableClient(selfRef)
	}
	return nil
}

func (selfRef *Client) reconnect() ClientProtocol {
	selfRef.glock.Lock()
	if selfRef.reconnectWaiter != nil {
		selfRef.reconnectWaiter = make(chan bool, 1)
	}
	if selfRef.unavailableWaiter != nil {
		close(selfRef.unavailableWaiter)
		selfRef.unavailableWaiter = nil
	}
	selfRef.glock.Unlock()

	for !selfRef.closed {
		select {
		case <-selfRef.reconnectWaiter:
			continue
		case <-time.After(3 * time.Second):
			err := selfRef.connect(selfRef.serverAddress, selfRef.clientId)
			if err != nil {
				continue
			}

			selfRef.glock.Lock()
			selfRef.reconnectWaiter = nil
			selfRef.glock.Unlock()
			selfRef.reconnectUpdateSubcribers()
			return nil
		}
	}

	selfRef.glock.Lock()
	selfRef.reconnectWaiter = nil
	selfRef.glock.Unlock()
	return nil
}

func (selfRef *Client) reconnectUpdateSubcribers() {
	selfRef.subscribeLock.Lock()
	for _, subscriber := range selfRef.subscribes {
		go func(subscriber *Subscriber) {
			err := selfRef.updateSubscription(subscriber)
			if err != nil {
				_ = selfRef.closeSubscribe(subscriber)
			}
		}(subscriber)
	}
	selfRef.subscribeLock.Unlock()
}

func (selfRef *Client) initProtocol(clientProtocol ClientProtocol,
									clientId [16]byte) error {
	initCommand := &protocol.InitCommand{Command: protocol.Command{Magic: protocol.MAGIC,
										 Version: protocol.VERSION, CommandType: protocol.COMMAND_INIT,
										 RequestId: selfRef.generateRequestId()}, ClientId: clientId}
	if err := clientProtocol.write(initCommand); err != nil {
		return err
	}
	result, rerr := clientProtocol.read()
	if rerr != nil {
		return rerr
	}

	if initResultCommand, ok := result.(*protocol.InitResultCommand); ok {
		return selfRef.handleInitCommandResult(initResultCommand)
	}
	return errors.New("init fail")
}

func (selfRef *Client) handleInitCommandResult(initResultCommand *protocol.InitResultCommand) error {
	if initResultCommand.Result != protocol.RESULT_SUCCEED {
		return errors.New(fmt.Sprintf("init stream error: %d", initResultCommand.Result))
	}

	if initResultCommand.InitType != 0 && initResultCommand.InitType != 2 {
		selfRef.requestLocker.Lock()
		for requestId := range selfRef.requests {
			close(selfRef.requests[requestId])
			delete(selfRef.requests, requestId)
		}
		selfRef.requestLocker.Unlock()
	}
	return nil
}

func (selfRef *Client) process() {
	for !selfRef.closed {
		if selfRef.clientProtocol == nil {
			err := selfRef.reconnect()
			if err != nil {
				return
			}
			continue
		}

		command, err := selfRef.clientProtocol.read()
		if err != nil {
			selfRef.glock.Lock()
			_ = selfRef.clientProtocol.close()
			if selfRef.replSet != nil {
				selfRef.replSet.removeAvailableClient(selfRef)
			}
			selfRef.clientProtocol = nil
			selfRef.glock.Unlock()
			err := selfRef.reconnect()
			if err != nil {
				return
			}
			continue
		}

		if command != nil {
			_ = selfRef.handleCommand(command.(protocol.ICommand))
		}
	}

	selfRef.glock.Lock()
	if selfRef.clientProtocol != nil {
		_ = selfRef.clientProtocol.close()
		selfRef.clientProtocol = nil
	}
	selfRef.glock.Unlock()
	if selfRef.replSet != nil {
		selfRef.replSet.removeAvailableClient(selfRef)
	}
	close(selfRef.closedWaiter)
}

func (selfRef *Client) getOrNewDB(dbId uint8) *Database {
	if selfRef.replSet != nil {
		return selfRef.replSet.getOrNewDB(dbId)
	}

	selfRef.dbLock.Lock()
	db := selfRef.dbs[dbId]
	if db == nil {
		db = BuildNewDatabase(dbId, selfRef)
		selfRef.dbs[dbId] = db
	}
	selfRef.dbLock.Unlock()
	return db
}

func (selfRef *Client) handleCommand(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_INIT:
		initCommand := command.(*protocol.InitResultCommand)
		return selfRef.handleInitCommandResult(initCommand)
	case protocol.COMMAND_PUBLISH:
		lockCommand := command.(*protocol.LockResultCommand)
		subscribeId := uint32(lockCommand.RequestId[12]) | uint32(lockCommand.RequestId[13])<<8 | uint32(lockCommand.RequestId[14])<<16 | uint32(lockCommand.RequestId[15])<<24
		selfRef.subscribeLock.Lock()
		if subscriber, ok := selfRef.subscribes[subscribeId]; ok {
			selfRef.subscribeLock.Unlock()
			return subscriber.Push(lockCommand)
		}
		return nil
	}

	requestId := command.GetRequestId()
	selfRef.requestLocker.Lock()
	if request, ok := selfRef.requests[requestId]; ok {
		delete(selfRef.requests, requestId)
		selfRef.requestLocker.Unlock()

		request <- command
		return nil
	}
	selfRef.requestLocker.Unlock()
	return nil
}

func (selfRef *Client) selectDB(dbId uint8) *Database {
	if selfRef.replSet != nil {
		return selfRef.replSet.selectDB(dbId)
	}

	db := selfRef.dbs[dbId]
	if db == nil {
		db = selfRef.getOrNewDB(dbId)
	}
	return db
}

func (selfRef *Client) executeCommand(command protocol.ICommand, timeout time.Duration) (protocol.ICommand, error) {
	selfRef.requestLocker.Lock()
	requestId := command.GetRequestId()
	if _, ok := selfRef.requests[requestId]; ok {
		selfRef.requestLocker.Unlock()
		return nil, errors.New("request is used")
	}
	waiter := make(chan protocol.ICommand, 1)
	selfRef.requests[requestId] = waiter
	selfRef.requestLocker.Unlock()

	selfRef.glock.Lock()
	if selfRef.clientProtocol == nil {
		selfRef.glock.Unlock()
		return nil, errors.New("client is not opened")
	}
	err := selfRef.clientProtocol.write(command)
	selfRef.glock.Unlock()
	if err != nil {
		selfRef.requestLocker.Lock()
		if _, ok := selfRef.requests[requestId]; ok {
			delete(selfRef.requests, requestId)
		}
		selfRef.requestLocker.Unlock()
		return nil, err
	}

	select {
	case r := <-waiter:
		if r == nil {
			return nil, errors.New("wait timeout")
		}
		return r, nil
	case <-time.After(time.Duration(timeout+1) * time.Second):
		selfRef.requestLocker.Lock()
		if _, ok := selfRef.requests[requestId]; ok {
			delete(selfRef.requests, requestId)
		}
		selfRef.requestLocker.Unlock()
		return nil, errors.New("timeout")
	}
}

func (selfRef *Client) sendCommand(command protocol.ICommand) error {
	selfRef.glock.Lock()
	if selfRef.clientProtocol == nil {
		selfRef.glock.Unlock()
		return errors.New("client is not opened")
	}
	err := selfRef.clientProtocol.write(command)
	selfRef.glock.Unlock()
	return err
}

func (selfRef *Client) lock(lockKey [16]byte, timeout uint32, expired uint32) *Lock {
	return selfRef.selectDB(0).lock(lockKey, time.Duration(timeout), expired)
}

func (selfRef *Client) event(eventKey [16]byte, timeout uint32, expired uint32, defaultSeted bool) *Event {
	return selfRef.selectDB(0).event(eventKey, time.Duration(timeout), expired, defaultSeted)
}

func (selfRef *Client) groupEvent(groupKey [16]byte, clientId uint64, versionId uint64, timeout uint32, expried uint32) *GroupEvent {
	return selfRef.selectDB(0).groupEvent(groupKey, clientId, versionId, time.Duration(timeout), expried)
}

func (selfRef *Client) semaphore(semaphoreKey [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
	return selfRef.selectDB(0).semaphore(semaphoreKey, time.Duration(timeout), expried, count)
}

func (selfRef *Client) readerWriterLock(lockKey [16]byte, timeout uint32, expried uint32) *RWLock {
	return selfRef.selectDB(0).rwLock(lockKey, time.Duration(timeout), expried)
}

func (selfRef *Client) readerLock(lockKey [16]byte, timeout uint32, expired uint32) *RLock {
	return selfRef.selectDB(0).rLock(lockKey, time.Duration(timeout), expired)
}

func (selfRef *Client) maxConcurrentFlow(flowKey [16]byte, count uint16, timeout uint32, expired uint32) *MaxConcurrentFlow {
	return selfRef.selectDB(0).maxConcurrentFlow(flowKey, count, time.Duration(timeout), expired)
}

func (selfRef *Client) tokenBucketFlow(flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	return selfRef.selectDB(0).tokenBucketFlow(flowKey, count, time.Duration(timeout), period)
}

func (selfRef *Client) treeLock(lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock {
	return selfRef.selectDB(0).treeLock(lockKey, parentKey, timeout, expried)
}

func (selfRef *Client) state(dbId uint8) *protocol.StateResultCommand {
	return selfRef.selectDB(dbId).State()
}

func (selfRef *Client) Subscribe(expried uint32, maxSize uint32) (ISubscriber, error) {
	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	return selfRef.subscribeMask(lockKeyMask, expried, maxSize)
}

func (selfRef *Client) subscribeMask(lockKeyMask [16]byte, expired uint32, maxSize uint32) (ISubscriber, error) {
	selfRef.subscribeLock.Lock()
	client.subscriberClientIdIndex++
	clientId := client.subscriberClientIdIndex
	selfRef.subscribeLock.Unlock()
	command := protocol.NewSubscribeCommand(clientId, 0, 0, lockKeyMask, expired, maxSize)
	resultCommand, err := selfRef.executeCommand(command, 5)
	if err != nil {
		return nil, err
	}
	subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand)
	if !ok {
		return nil, errors.New("unknown command")
	}
	if subscribeResultCommand.Result != protocol.RESULT_SUCCEED {
		return nil, errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
	}

	subscriber := NewSubscriber(selfRef, clientId, subscribeResultCommand.SubscribeId, lockKeyMask, expired, maxSize)
	selfRef.subscribeLock.Lock()
	selfRef.subscribes[subscriber.subscribeId] = subscriber
	selfRef.subscribeLock.Unlock()
	return subscriber, nil
}

func (selfRef *Client) closeSubscribe(isubscriber ISubscriber) error {
	isubscriber = isubscriber.(*Subscriber)
	if isubscriber.closed {
		return nil
	}

	command := protocol.NewSubscribeCommand(isubscriber.clientId, isubscriber.subscribeId, 1, isubscriber.lockKeyMask, isubscriber.expried, isubscriber.maxSize)
	resultCommand, err := selfRef.executeCommand(command, 5)
	if err != nil {
		return err
	}
	subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand)
	if !ok {
		return errors.New("unknown command")
	}
	if subscribeResultCommand.Result != protocol.RESULT_SUCCEED {
		return errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
	}

	selfRef.subscribeLock.Lock()
	if _, ok := selfRef.subscribes[isubscriber.subscribeId]; ok {
		delete(selfRef.subscribes, isubscriber.subscribeId)
	}
	isubscriber.closed = true
	close(isubscriber.channel)
	close(isubscriber.closedWaiter)
	selfRef.subscribeLock.Unlock()
	if isubscriber.replset != nil {
		hasAvailable := false
		for _, s := range isubscriber.replset.subscribers {
			if !s.closed {
				hasAvailable = true
			}
		}
		if !hasAvailable {
			_ = isubscriber.replset.Close()
		}
	}
	return nil
}

func (selfRef *Client) updateSubscription(subcriber ISubscriber) error {
	subscriberRegistry := subcriber.(*Subscriber)
	if subscriberRegistry.closed {
		return nil
	}

	command := protocol.NewSubscribeCommand(subscriberRegistry.clientId, subscriberRegistry.subscribeId, 0, subscriberRegistry.lockKeyMask, subscriberRegistry.expried, subscriberRegistry.maxSize)
	resultCommand, err := selfRef.executeCommand(command, 5)
	if err != nil {
		return err
	}
	subscribeResultCommand, ok := resultCommand.(*protocol.SubscribeResultCommand)
	if !ok {
		return errors.New("unknown command")
	}
	if subscribeResultCommand.Result != protocol.RESULT_SUCCEED {
		return errors.New(fmt.Sprintf("command error: code %d", subscribeResultCommand.Result))
	}
	subscriberRegistry.subscribeId = subscribeResultCommand.SubscribeId
	return nil
}

func (selfRef *Client) generateRequestId() [16]byte {
	return protocol.GenRequestId()
}

func (selfRef *Client) Unavailable() chan bool {
	selfRef.glock.Lock()
	if selfRef.unavailableWaiter == nil {
		selfRef.unavailableWaiter = make(chan bool, 1)
	}
	selfRef.glock.Unlock()
	return selfRef.unavailableWaiter
}

type ReplsetClient struct {
	glock             *sync.Mutex
	clients           []*Client
	availableClients  []*Client
	dbs               []*Database
	dbLock            *sync.Mutex
	closed            bool
	closedWaiter      chan bool
	unavailableWaiter chan bool
}

func NewReplsetClient(hosts []string) *ReplsetClient {
	replsetClient := &ReplsetClient{&sync.Mutex{}, make([]*Client, 0), make([]*Client, 0),
		make([]*Database, 256), &sync.Mutex{}, false, make(chan bool, 1), nil}

	for _, host := range hosts {
		clientRegistry := &Client{&sync.Mutex{}, replsetClient, nil, replsetClient.dbs,
			replsetClient.dbLock, make(map[[16]byte]chan protocol.ICommand, 64), &sync.Mutex{},
			make(map[uint32]*Subscriber, 4), &sync.Mutex{}, host, protocol.GenClientId(),
			false, make(chan bool, 1), nil, nil}
		replsetClient.clients = append(replsetClient.clients, clientRegistry)
	}
	return replsetClient
}

func (selfRef *ReplsetClient) open() error {
	if len(selfRef.clients) == 0 {
		return errors.New("not client")
	}

	var clientError error = nil
	for _, client := range selfRef.clients {
		err := client.open()
		if err != nil {
			clientError = err
			go client.process()
		}
	}

	if len(selfRef.availableClients) == 0 {
		return clientError
	}
	return nil
}

func (selfRef *ReplsetClient) close() error {
	selfRef.glock.Lock()
	if selfRef.closed {
		selfRef.glock.Unlock()
		return nil
	}
	selfRef.closed = true
	selfRef.glock.Unlock()
	for _, client := range selfRef.clients {
		_ = client.close()
	}

	selfRef.dbLock.Lock()
	for dbId, db := range selfRef.dbs {
		if db != nil {
			_ = db.close()
			selfRef.dbs[dbId] = nil
		}
	}
	selfRef.dbLock.Unlock()

	close(selfRef.closedWaiter)
	if selfRef.unavailableWaiter != nil {
		close(selfRef.unavailableWaiter)
		selfRef.unavailableWaiter = nil
	}
	return nil
}

func (selfRef *ReplsetClient) addAvailableClient(client *Client) {
	selfRef.glock.Lock()
	selfRef.availableClients = append(selfRef.availableClients, client)
	selfRef.glock.Unlock()
}

func (selfRef *ReplsetClient) removeAvailableClient(client *Client) {
	selfRef.glock.Lock()
	availableClients := make([]*Client, 0)
	for _, c := range selfRef.availableClients {
		if c != client {
			availableClients = append(availableClients, c)
		}
	}
	selfRef.availableClients = availableClients
	if len(selfRef.availableClients) == 0 {
		if selfRef.unavailableWaiter != nil {
			close(selfRef.unavailableWaiter)
			selfRef.unavailableWaiter = nil
		}
	}
	selfRef.glock.Unlock()
}

func (selfRef *ReplsetClient) getClient() *Client {
	selfRef.glock.Lock()
	if len(selfRef.availableClients) > 0 {
		client := selfRef.availableClients[0]
		selfRef.glock.Unlock()
		return client
	}
	selfRef.glock.Unlock()
	return nil
}

func (selfRef *ReplsetClient) getOrNewDB(dbId uint8) *Database {
	selfRef.dbLock.Lock()
	db := selfRef.dbs[dbId]
	if db == nil {
		db = BuildNewDatabase(dbId, selfRef)
		selfRef.dbs[dbId] = db
	}
	selfRef.dbLock.Unlock()
	return db
}

func (selfRef *ReplsetClient) selectDB(dbId uint8) *Database {
	db := selfRef.dbs[dbId]
	if db == nil {
		db = selfRef.getOrNewDB(dbId)
	}
	return db
}

func (selfRef *ReplsetClient) executeCommand(command protocol.ICommand, timeout time.Duration) (protocol.ICommand, error) {
	client := selfRef.getClient()
	if client == nil {
		return nil, errors.New("Clients unavailable")
	}
	return client.executeCommand(command, timeout)
}

func (selfRef *ReplsetClient) sendCommand(command protocol.ICommand) error {
	client := selfRef.getClient()
	if client == nil {
		return errors.New("Clients unavailable")
	}
	return client.sendCommand(command)
}

func (selfRef *ReplsetClient) lock(lockKey [16]byte, timeout uint32, expired uint32) *Lock {
	return selfRef.selectDB(0).lock(lockKey, timeout, expired)
}

func (selfRef *ReplsetClient) event(eventKey [16]byte, timeout uint32, expired uint32, defaultSeted bool) *Event {
	return selfRef.selectDB(0).event(eventKey, time.Duration(timeout), expired, defaultSeted)
}

func (selfRef *ReplsetClient) groupEvent(groupKey [16]byte, clientId uint64, versionId uint64, timeout uint32, expried uint32) *GroupEvent {
	return selfRef.selectDB(0).groupEvent(groupKey, clientId, versionId, time.Duration(timeout), expried)
}

func (selfRef *ReplsetClient) semaphore(semaphoreKey [16]byte, timeout uint32, expired uint32, count uint16) *Semaphore {
	return selfRef.selectDB(0).semaphore(semaphoreKey, time.Duration(timeout), expired, count)
}

func (selfRef *ReplsetClient) readerWriterLock(lockKey [16]byte, timeout uint32, expired uint32) *RWLock {
	return selfRef.selectDB(0).rwLock(lockKey, time.Duration(timeout), expired)
}

func (selfRef *ReplsetClient) rLock(lockKey [16]byte, timeout uint32, expired uint32) *RLock {
	return selfRef.selectDB(0).rLock(lockKey, time.Duration(timeout), expired)
}

func (selfRef *ReplsetClient) maxConcurrentFlow(flowKey [16]byte, count uint16, timeout uint32, expired uint32) *MaxConcurrentFlow {
	return selfRef.selectDB(0).maxConcurrentFlow(flowKey, count, time.Duration(timeout), expired)
}

func (selfRef *ReplsetClient) tokenBucketFlow(flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	return selfRef.selectDB(0).tokenBucketFlow(flowKey, count, time.Duration(timeout), period)
}

func (selfRef *ReplsetClient) treeLock(lockKey [16]byte, parentKey [16]byte, timeout uint32, expired uint32) *TreeLock {
	return selfRef.selectDB(0).treeLock(lockKey, parentKey, timeout, expired)
}

func (selfRef *ReplsetClient) subscribe(expired uint32, maxSize uint32) (ISubscriber, error) {
	lockKeyMask := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	return selfRef.SubscribeMask(lockKeyMask, expired, maxSize)
}

func (selfRef *ReplsetClient) SubscribeMask(lockKeyMask [16]byte, expried uint32, maxSize uint32) (ISubscriber, error) {
	replsetSubscriber := NewReplsetSubscriber(selfRef, lockKeyMask, expried, maxSize)
	var err error = nil
	for _, client := range selfRef.availableClients {
		subscriber, cerr := client.subscribeMask(lockKeyMask, expried, maxSize)
		if cerr != nil {
			err = cerr
			continue
		}
		_ = replsetSubscriber.addSubscriber(subscriber.(*client.Subscriber))
	}

	if len(replsetSubscriber.subscribers) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("empty clients")
	}
	return replsetSubscriber, nil
}

func (selfRef *ReplsetClient) closeSubscriber(subscriber ISubscriber) error {
	replsetSubscriber := subscriber.(*ReplsetSubscriber)
	if replsetSubscriber.closed {
		return nil
	}

	for _, subscriber := range replsetSubscriber.subscribers {
		_ = subscriber.client.closeSubscribe(subscriber)
	}

	selfRef.glock.Lock()
	replsetSubscriber.closed = true
	close(replsetSubscriber.channel)
	close(replsetSubscriber.closedWaiter)
	selfRef.glock.Unlock()
	return nil
}

func (selfRef *ReplsetClient) updateSubscriber(subscriber ISubscriber) error {
	replsetSubscriber := subscriber.(*ReplsetSubscriber)
	if replsetSubscriber.closed {
		return nil
	}

	for _, subscriber := range replsetSubscriber.subscribers {
		_ = subscriber.client.updateSubscription(subscriber)
	}
	return nil
}

func (selfRef *ReplsetClient) generateRequestId() [16]byte {
	return protocol.GenRequestId()
}

func (selfRef *ReplsetClient) unavailable() chan bool {
	selfRef.glock.Lock()
	if selfRef.unavailableWaiter == nil {
		selfRef.unavailableWaiter = make(chan bool, 1)
	}
	selfRef.glock.Unlock()
	return selfRef.unavailableWaiter
}
