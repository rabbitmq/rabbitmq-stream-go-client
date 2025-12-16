package stream

import (
	"sync"
	"time"
)

type clientConnectionParameters struct {
	connectionName    string
	broker            *Broker
	tcpParameters     *TCPParameters
	saslConfiguration *SaslConfiguration
	rpcTimeOut        time.Duration
}

type IEntity interface {
	GetId() uint8
	Close() error
}

type IClient interface {
	// Close TODO: Maybe add error return value
	Close()
	Entities() []IEntity
	AddEntity(IEntity)
	RemoveEntityById(id uint8)
	GetUniqueId() string
	connect() error
}

type ClientPool struct {
	key    string
	client IClient
}

type ClientPools struct {
	mutex    *sync.Mutex
	pools    []*ClientPool
	maxItems int
}

func NewClientPools(maxItems int) *ClientPools {
	return &ClientPools{
		mutex:    &sync.Mutex{},
		pools:    make([]*ClientPool, 0),
		maxItems: maxItems,
	}
}

// AddEntityAndGetConnection adds a new client pool if the key does not already exist
// or if the client reached the maximum number of entities
func (cp *ClientPools) AddEntityAndGetConnection(key string, entity IEntity, parameters clientConnectionParameters, fn func(clientConnectionParameters) (IClient, error)) (IClient, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, pool := range cp.pools {
		if pool.key == key && len(pool.client.Entities()) < cp.maxItems {
			pool.client.AddEntity(entity)
			return pool.client, nil
		}
	}

	client, err := fn(clientConnectionParameters{

		connectionName:    parameters.connectionName,
		broker:            parameters.broker,
		tcpParameters:     parameters.tcpParameters,
		saslConfiguration: parameters.saslConfiguration,
		rpcTimeOut:        parameters.rpcTimeOut,
	})
	if err != nil {
		return nil, err
	}
	cp.pools = append(cp.pools, &ClientPool{
		key:    key,
		client: client,
	})

	// Subscribe to close events
	client.AddEntity(entity)
	return client, nil
}

// RemoveEntityIdFromClientId the item id from the client with the given key
func (cp *ClientPools) RemoveEntityIdFromClientId(uniqueId string, id uint8) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, pool := range cp.pools {
		if pool.client.GetUniqueId() == uniqueId {
			pool.client.RemoveEntityById(id)
			break
		}
	}
	cp.garbageCollect()
}

// RemoveClient removes the client with the given key
func (cp *ClientPools) RemoveClient(uniqueId string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	activePools := make([]*ClientPool, 0)
	for _, pool := range cp.pools {
		if pool.client.GetUniqueId() == uniqueId {
			pool.client.Close()
		} else {
			activePools = append(activePools, pool)
		}
	}
	cp.pools = activePools
}

// GarbageCollect closes clients that have no more entities
func (cp *ClientPools) garbageCollect() {
	activePools := make([]*ClientPool, 0)
	for _, pool := range cp.pools {
		if len(pool.client.Entities()) > 0 {
			activePools = append(activePools, pool)
		} else {
			pool.client.Close()
		}
	}
	cp.pools = activePools
}

// Count returns the number of pools
func (cp *ClientPools) Count() int {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	return len(cp.pools)
}

// Close all clients
func (cp *ClientPools) Close() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, pool := range cp.pools {
		pool.client.Close()
	}
	cp.pools = make([]*ClientPool, 0)
}

type EntitiesPool struct {
	producers *ClientPools
	consumers *ClientPools
}

func NewEntitiesPool(maxProducersPerClient int, maxConsumersPerClient int) *EntitiesPool {
	return &EntitiesPool{
		producers: NewClientPools(maxProducersPerClient),
		consumers: NewClientPools(maxConsumersPerClient),
	}
}
