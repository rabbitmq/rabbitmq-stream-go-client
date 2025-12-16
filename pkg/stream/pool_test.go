package stream

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type fakeEntity struct {
	id uint8
}

func (f *fakeEntity) GetId() uint8 { return f.id }
func (f *fakeEntity) Close() error { return nil }

type fakeClient struct {
	mu       sync.Mutex
	entities []IEntity
	uid      string
	closed   bool
}

func newFakeClient(uid string) *fakeClient {
	return &fakeClient{
		uid:      uid,
		entities: make([]IEntity, 0),
	}
}

func (f *fakeClient) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeClient) Entities() []IEntity {
	f.mu.Lock()
	defer f.mu.Unlock()
	cpy := make([]IEntity, len(f.entities))
	copy(cpy, f.entities)
	return cpy
}

func (f *fakeClient) AddEntity(e IEntity) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entities = append(f.entities, e)
}

func (f *fakeClient) RemoveEntityById(id uint8) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < len(f.entities); i++ {
		if f.entities[i].GetId() == id {
			f.entities = append(f.entities[:i], f.entities[i+1:]...)
			return
		}
	}
}

func (f *fakeClient) GetUniqueId() string { return f.uid }
func (f *fakeClient) connect() error      { return nil }

var _ = Describe("ClientPools with Fake client", func() {

	newClientFactory := func() func(clientConnectionParameters) (IClient, error) {
		return func(clientConnectionParameters) (IClient, error) {
			uuid := uuid.New().String()
			return newFakeClient(uuid), nil
		}
	}

	It("reuses a client until it reaches maxItems then creates a new one", func() {
		cp := NewClientPools(2)
		clientFactory := newClientFactory()

		c1, err := cp.AddEntityAndGetConnection("key", &fakeEntity{id: 1}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		c2, err := cp.AddEntityAndGetConnection("key", &fakeEntity{id: 2}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(c2.GetUniqueId()).To(Equal(c1.GetUniqueId()))

		c3, err := cp.AddEntityAndGetConnection("key", &fakeEntity{id: 3}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(c3.GetUniqueId()).NotTo(Equal(c1.GetUniqueId()))
	})

	It("removes an entity from a client by unique id", func() {
		cp := NewClientPools(2)
		clientFactory := newClientFactory()

		client, err := cp.AddEntityAndGetConnection("k2", &fakeEntity{id: 5}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(client.Entities())).To(Equal(1))

		cp.RemoveEntityIdFromClientId(client.GetUniqueId(), 5)
		Expect(len(client.Entities())).To(Equal(0))
	})

	It("removes and closes a client by unique id", func() {
		cp := NewClientPools(2)
		clientFactory := newClientFactory()

		clientIfc, err := cp.AddEntityAndGetConnection("k3", &fakeEntity{id: 7}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(cp.Count()).To(Equal(1))

		fc := clientIfc.(*fakeClient)
		cp.RemoveClient(fc.GetUniqueId())
		Expect(cp.Count()).To(Equal(0))
		Expect(fc.closed).To(BeTrue())
	})

	It("garbage collects clients without entities", func() {
		cp := NewClientPools(2)
		clientFactory := newClientFactory()

		c1, err := cp.AddEntityAndGetConnection("g1", &fakeEntity{id: 1}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		c2, err := cp.AddEntityAndGetConnection("g2", &fakeEntity{id: 2}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())

		cp.RemoveEntityIdFromClientId(c1.GetUniqueId(), 1)
		Expect(cp.Count()).To(Equal(1))

		remaining := cp.pools[0].client
		Expect(remaining.GetUniqueId()).To(Equal(c2.GetUniqueId()))
	})

	It("closes all clients on Close", func() {
		cp := NewClientPools(2)
		clientFactory := newClientFactory()

		c1, err := cp.AddEntityAndGetConnection("x1", &fakeEntity{id: 1}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		c2, err := cp.AddEntityAndGetConnection("x2", &fakeEntity{id: 2}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())

		fc1 := c1.(*fakeClient)
		fc2 := c2.(*fakeClient)

		cp.Close()
		Expect(cp.Count()).To(Equal(0))
		Expect(fc1.closed).To(BeTrue())
		Expect(fc2.closed).To(BeTrue())
	})
	It("integration test multiple scenarios", func() {
		cp := NewClientPools(7)
		clientFactory := newClientFactory()
		firstClientId := ""
		// since the max items is 7, all entities should go to the same client
		for i := 0; i < 7; i++ {
			key := "localhost:5552"
			client, err := cp.AddEntityAndGetConnection(key, &fakeEntity{id: uint8(i)}, clientConnectionParameters{}, clientFactory)
			Expect(err).NotTo(HaveOccurred())
			Expect(client).NotTo(BeNil())
			Expect(cp.pools[0].client.GetUniqueId()).To(Equal(client.GetUniqueId()))
			firstClientId = client.GetUniqueId()
		}
		Expect(cp.Count()).To(Equal(1))

		// adding one more entity should create a new client
		client, err := cp.AddEntityAndGetConnection("localhost:5552", &fakeEntity{id: 8}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(client).NotTo(BeNil())
		Expect(cp.Count()).To(Equal(2))
		// we remove an entity from the first client
		// this should not remove the client since other entities exist
		cp.RemoveEntityIdFromClientId(firstClientId, 3)
		Expect(cp.Count()).To(Equal(2)) // still 2 since other entities exist
		Expect(len(cp.pools[0].client.Entities())).To(Equal(6))

		// removing all entities from the first client should remove it
		for i := 0; i < 7; i++ {
			if i == 3 {
				continue // already removed
			}
			cp.RemoveEntityIdFromClientId(firstClientId, uint8(i))
		}
		// removing the last entity should remove the last client
		Expect(cp.Count()).To(Equal(1)) // first client should be removed
		// since it is removed, the client id should be different
		Expect(cp.pools[0].client.GetUniqueId()).NotTo(Equal(firstClientId))

		lastClientId := cp.pools[0].client.GetUniqueId()
		cp.RemoveEntityIdFromClientId(lastClientId, 8)
		Expect(cp.Count()).To(Equal(0)) // all clients should be removed

		// adding a new entity should create a new client
		client, err = cp.AddEntityAndGetConnection("localhost:5552", &fakeEntity{id: 9}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(client).NotTo(BeNil())
		Expect(cp.Count()).To(Equal(1))
		Expect(cp.pools[0].client.GetUniqueId()).NotTo(Equal(lastClientId))
		// removing the client directly should close it
		cp.RemoveClient(client.GetUniqueId())
		Expect(cp.Count()).To(Equal(0))

	})

	It("handles concurrent AddEntityAndGetConnection safely", func() {
		cp := NewClientPools(10)
		clientFactory := newClientFactory()

		var wg sync.WaitGroup
		const goroutines = 50
		const addsPerG = 20

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < addsPerG; i++ {
					key := fmt.Sprintf("k-%d", gid%5)
					_, err := cp.AddEntityAndGetConnection(key, &fakeEntity{id: uint8(i % 255)},
						clientConnectionParameters{}, clientFactory)
					Expect(err).NotTo(HaveOccurred())
				}
			}(g)
		}
		wg.Wait()

		total := 0
		cp.mutex.Lock()
		for _, p := range cp.pools {
			total += len(p.client.Entities())
		}
		cp.mutex.Unlock()

		Expect(total).To(Equal(goroutines * addsPerG))
	})
})

/// test with the real TCP client

var _ = Describe("ClientPools with the TCP client", Focus, func() {
	It("creates and reuses TCP clients correctly", func() {
		cp := NewClientPools(3)
		clientFactory := func(parameters clientConnectionParameters) (IClient, error) {
			return newClient(parameters), nil
		}

		c1, err := cp.AddEntityAndGetConnection("tcp-key", &fakeEntity{id: 1}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(c1.connect()).To(Succeed())
		c2, err := cp.AddEntityAndGetConnection("tcp-key", &fakeEntity{id: 2}, clientConnectionParameters{}, clientFactory)
		Expect(err).NotTo(HaveOccurred())
		Expect(c2.connect()).To(Succeed())
		Expect(c2.GetUniqueId()).To(Equal(c1.GetUniqueId()))
		c2.Close()
	})
	It("creates and reuses TCP clients correctly in multi-threading", func() {
		cp := NewClientPools(5)
		clientFactory := func(parameters clientConnectionParameters) (IClient, error) {
			return newClient(parameters), nil
		}

		var wg sync.WaitGroup
		const goroutines = 20
		const addsPerG = 10

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < addsPerG; i++ {
					key := "tcp-multi-key"
					client, err := cp.AddEntityAndGetConnection(key, &fakeEntity{id: uint8(i % 255)},
						clientConnectionParameters{
							connectionName: fmt.Sprintf("client-%d-%d", gid, i),
							rpcTimeOut:     time.Duration(10) * time.Second,
						}, clientFactory)
					Expect(err).NotTo(HaveOccurred())
					err = client.connect()
					Expect(err).NotTo(HaveOccurred())
				}
			}(g)
		}
		wg.Wait()

		total := 0
		cp.mutex.Lock()
		for _, p := range cp.pools {
			total += len(p.client.Entities())
		}
		cp.mutex.Unlock()
	})

	It("creates and reuses TCP clients correctly with multiple keys", func() {
		cp := NewClientPools(4)
		clientFactory := func(parameters clientConnectionParameters) (IClient, error) {
			return newClient(parameters), nil
		}

		keys := []string{"tcp-key-1", "tcp-key-2", "tcp-key-3"}

		for _, key := range keys {
			for i := 0; i < 4; i++ {
				client, err := cp.AddEntityAndGetConnection(key, &fakeEntity{id: uint8(i)},
					clientConnectionParameters{
						connectionName: fmt.Sprintf("client-%s-%d", key, i),
						rpcTimeOut:     time.Duration(10) * time.Second,
					}, clientFactory)
				Expect(err).NotTo(HaveOccurred())
				err = client.connect()
				Expect(err).NotTo(HaveOccurred())
			}
		}

		Expect(cp.Count()).To(Equal(len(keys)))

		for _, key := range keys {
			found := false
			for _, pool := range cp.pools {
				if pool.key == key {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), fmt.Sprintf("Expected to find pool for key %s", key))
		}
	})

})
