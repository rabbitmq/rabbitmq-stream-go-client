package stream

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
	"time"
)

var _ = Describe("Environment test", func() {

	It("Multi Producers", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		var producers []*Producer

		for i := 0; i < 10; i++ {
			producer, err := env.NewProducer(streamName, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.ID).To(Equal(uint8(i % 3)))
			producers = append(producers, producer)
		}

		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(4))

		for _, producer := range producers {
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(len(env.producers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(0))

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multi Producers per client", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxProducersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			producer, err := env.NewProducer(streamName, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.ID).To(Equal(uint8(i % 2)))
		}

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(len(env.producers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(0))

	})

	It("Multi Producers multi threads", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		wg := &sync.WaitGroup{}

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				producer, err := env.NewProducer(streamName, nil, nil)
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				err = producer.Close()
				Expect(err).NotTo(HaveOccurred())
				wg.Done()
			}(wg)
		}
		wg.Wait()
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(0))
		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Meta handler delete consistency", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamNameWillBeDelete := uuid.New().String()
		err = env.DeclareStream(streamNameWillBeDelete, nil)
		Expect(err).NotTo(HaveOccurred())

		streamNameWillBeDeleteAfter := uuid.New().String()
		err = env.DeclareStream(streamNameWillBeDeleteAfter, nil)
		Expect(err).NotTo(HaveOccurred())

		wg := &sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				_, errProd := env.NewProducer(streamNameWillBeDelete, nil, nil)
				Expect(errProd).NotTo(HaveOccurred())
				_, errProd = env.NewProducer(streamNameWillBeDeleteAfter, nil, nil)
				Expect(errProd).NotTo(HaveOccurred())
				wg.Done()
			}()

		}
		wg.Wait()
		err = env.DeleteStream(streamNameWillBeDelete)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(4))
		err = env.DeleteStream(streamNameWillBeDeleteAfter)
		time.Sleep(500 * time.Millisecond)
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(0))
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Environment Authentication", func() {
		It("Connection Authentication Failure", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-StreamOptions://wrong_user:wrong_password@localhost:5551/%2f"))
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("authentication failure"))
		})

		It("Connection Vhost not exist", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-StreamOptions://guest:guest@localhost:5551/VHOSTNOEXIST"))
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("virtualHost access failure"))
		})

		It("Connection No Endpoint", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-StreamOptions://g:g@noendpoint:5551/%2f"))
			Expect(err).To(HaveOccurred())
		})
	})
})
