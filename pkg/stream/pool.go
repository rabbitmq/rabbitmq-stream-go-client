package stream

import "sync"

const MaxIds = 200

type IEntity interface {
	GetId() uint8
}

type IClient interface {
	Close() error
	Entities() []IEntity
}

type ClientPool struct {
	key    string
	client IClient
}

type ClientPools struct {
	mutex *sync.Mutex
	pools []*ClientPool
}

func NewClientPools() *ClientPools {
	return &ClientPools{
		mutex: &sync.Mutex{},
		pools: make([]*ClientPool, 0),
	}
}

// MaybeAddPool adds a new client pool if the key does not already exist
// or if the client reached the maximum number of entities
func (cp *ClientPools) MaybeAddPool(key string, fn func() (IClient, error)) (IClient, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, pool := range cp.pools {
		if pool.key == key && len(pool.client.Entities()) < MaxIds {
			return pool.client, nil
		}
	}
	client, err := fn()
	if err != nil {
		return nil, err
	}
	cp.pools = append(cp.pools, &ClientPool{
		key:    key,
		client: client,
	})
	return client, nil
}

// GarbageCollect closes clients that have no more entities
func (cp *ClientPools) GarbageCollect() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	activePools := make([]*ClientPool, 0)
	for _, pool := range cp.pools {
		if len(pool.client.Entities()) == 0 {
			_ = pool.client.Close()
		} else {
			activePools = append(activePools, pool)
		}
	}
	cp.pools = activePools
}
