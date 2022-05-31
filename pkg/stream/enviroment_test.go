package stream

import (
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Environment test", func() {

	It("Multi Producers", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).NotTo(HaveOccurred())
		var producers []*Producer

		for i := 0; i < 10; i++ {
			producer, err := env.NewProducer(streamName, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.id).To(Equal(uint8(0)))
			producers = append(producers, producer)
		}

		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(10))

		for _, producer := range producers {
			Expect(producer.Close()).NotTo(HaveOccurred())
		}

		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))

		Expect(env.DeleteStream(streamName)).NotTo(HaveOccurred())
	})

	It("Multi Producers per client", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxProducersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).
			NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			producer, err := env.NewProducer(streamName, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(producer.id).To(Equal(uint8(i % 2)))
		}

		Expect(env.DeleteStream(streamName)).NotTo(HaveOccurred())

		time.Sleep(500 * time.Millisecond)
		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))

	})

	It("Producers multi threads", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).
			NotTo(HaveOccurred())
		wg := &sync.WaitGroup{}

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				producer, err := env.NewProducer(streamName, nil)
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(20 * time.Millisecond)
				Expect(producer.Close()).NotTo(HaveOccurred())
				wg.Done()
			}(wg)
		}
		wg.Wait()
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))
		Expect(env.DeleteStream(streamName)).NotTo(HaveOccurred())
	})

	It("Meta handler delete consistency threads", func() {
		env, err := NewEnvironment(&EnvironmentOptions{
			MaxProducersPerClient: 3,
			MaxConsumersPerClient: 3,
		})
		Expect(err).NotTo(HaveOccurred())
		streamNameWillBeDelete := uuid.New().String()
		Expect(env.DeclareStream(streamNameWillBeDelete, nil)).NotTo(HaveOccurred())

		streamNameWillBeDeleteAfter := uuid.New().String()
		Expect(env.DeclareStream(streamNameWillBeDeleteAfter, nil)).
			NotTo(HaveOccurred())

		wg := &sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				_, errProd := env.NewProducer(streamNameWillBeDelete, nil)
				Expect(errProd).NotTo(HaveOccurred())
				_, errProd = env.NewProducer(streamNameWillBeDeleteAfter, nil)
				Expect(errProd).NotTo(HaveOccurred())
				wg.Done()
			}()

		}
		wg.Wait()
		time.Sleep(500 * time.Millisecond)
		Expect(env.DeleteStream(streamNameWillBeDelete)).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		err = env.DeleteStream(streamNameWillBeDeleteAfter)
		time.Sleep(200 * time.Millisecond)
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))
		Expect(err).NotTo(HaveOccurred())

		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("Meta handler delete consistency sync", func() {
		env, err := NewEnvironment(&EnvironmentOptions{
			MaxProducersPerClient: 5,
			MaxConsumersPerClient: 5,
		})
		Expect(err).NotTo(HaveOccurred())
		streamNameWillBeDelete := uuid.New().String()
		err = env.DeclareStream(streamNameWillBeDelete, nil)
		Expect(err).NotTo(HaveOccurred())

		streamNameWillBeDeleteAfter := uuid.New().String()
		Expect(env.DeclareStream(streamNameWillBeDeleteAfter, nil)).
			NotTo(HaveOccurred())

		for i := 0; i < 25; i++ {
			_, errProd := env.NewProducer(streamNameWillBeDelete, nil)
			Expect(errProd).NotTo(HaveOccurred())
			_, errProd = env.NewProducer(streamNameWillBeDeleteAfter, nil)
			Expect(errProd).NotTo(HaveOccurred())
		}

		time.Sleep(500 * time.Millisecond)
		Expect(env.DeleteStream(streamNameWillBeDelete)).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(10))

		err = env.DeleteStream(streamNameWillBeDeleteAfter)
		time.Sleep(200 * time.Millisecond)
		Expect(len(env.producers.getCoordinators())).To(Equal(1))
		Expect(len(env.producers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))
		Expect(err).NotTo(HaveOccurred())

		Expect(env.Close()).NotTo(HaveOccurred())
	})

	Describe("Environment Authentication/Validation", func() {
		It("Connection Authentication Failure", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-stream://wrong_user:wrong_password@localhost:5552/%2f"))
			Expect(err).
				To(Equal(AuthenticationFailure))
		})

		It("Connection Vhost not exist", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-stream://guest:guest@localhost:5552/VHOSTNOEXIST"))
			Expect(err).To(Equal(VirtualHostAccessFailure))
		})

		It("Connection Vhost not exist", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().SetVHost("VHOSTNOEXIST"))
			Expect(err).To(Equal(VirtualHostAccessFailure))
		})

		It("Connection Vhost exists", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-stream://guest:guest@localhost:5552/" + testVhost))
			Expect(err).NotTo(HaveOccurred())
		})

		It("Connection No Endpoint", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-stream://g:g@noendpoint:5552/%2f"))
			Expect(err).To(HaveOccurred())
		})

	})

	Describe("TCP Parameters", func() {

		env, err := NewEnvironment(&EnvironmentOptions{
			ConnectionParameters: []*Broker{
				newBrokerDefault(),
			},
			TCPParameters: &TCPParameters{
				tlsConfig:             nil,
				RequestedHeartbeat:    60,
				RequestedMaxFrameSize: 1048574,
				WriteBuffer:           100,
				ReadBuffer:            200,
				NoDelay:               false,
			},
			MaxProducersPerClient: 1,
			MaxConsumersPerClient: 1,
			AddressResolver:       nil,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())

	})

	Describe("Environment Validations", func() {

		_, err := NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(0).
			SetMaxProducersPerClient(0))
		Expect(err).To(HaveOccurred())

		_, err = NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(500).
			SetMaxProducersPerClient(500))
		Expect(err).To(HaveOccurred())

		It("Malformed URI", func() {
			_, err := NewEnvironment(NewEnvironmentOptions().
				SetUri("rabbitmq-stream%%%malformed_uri"))
			Expect(err).To(HaveOccurred())
		})

		It("Merge with Default", func() {
			env2, err := NewEnvironment(NewEnvironmentOptions().SetHost("").
				SetUser("").SetPassword("").SetPort(0))
			Expect(err).NotTo(HaveOccurred())
			Expect(env2.Close()).NotTo(HaveOccurred())
		})

		It("ReadBuffer and WriteBuffer defaulted to non-zero values", func() {
			env, err := NewEnvironment(NewEnvironmentOptions())
			Expect(err).NotTo(HaveOccurred())
			Expect(env.options.TCPParameters.ReadBuffer).NotTo(BeZero())
			Expect(env.options.TCPParameters.WriteBuffer).NotTo(BeZero())
		})

		It("RequestedHeartbeat and RequestFrameSize defaulted to non-zero values", func() {
			env, err := NewEnvironment(NewEnvironmentOptions())
			Expect(err).NotTo(HaveOccurred())
			Expect(env.options.TCPParameters.RequestedHeartbeat).NotTo(BeZero())
			Expect(env.options.TCPParameters.RequestedMaxFrameSize).NotTo(BeZero())
		})

	})

	Describe("Stream Existing/Meta data", func() {

		env, err := NewEnvironment(NewEnvironmentOptions().SetPort(5552).
			SetUser("guest").
			SetPassword("guest").SetHost("localhost"))
		Expect(err).NotTo(HaveOccurred())
		stream := uuid.New().String()
		err = env.DeclareStream(stream, nil)
		Expect(err).NotTo(HaveOccurred())
		exists, err := env.StreamExists(stream)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(Equal(true))
		metaData, err := env.StreamMetaData(stream)
		Expect(err).NotTo(HaveOccurred())
		Expect(metaData.Leader.Host).To(Equal("localhost"))
		Expect(metaData.Leader.Port).To(Equal("5552"))
		Expect(len(metaData.Replicas)).To(Equal(0))
		Expect(env.DeleteStream(stream)).NotTo(HaveOccurred())
		exists, err = env.StreamExists(stream)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(Equal(false))
		Expect(env.Close()).NotTo(HaveOccurred())

	})

	Describe("Address Resolver", func() {
		addressResolver := AddressResolver{
			Host: "localhost",
			Port: 5552,
		}
		env, err := NewEnvironment(
			NewEnvironmentOptions().
				SetHost(addressResolver.Host).
				SetPort(addressResolver.Port).
				SetAddressResolver(addressResolver).
				SetMaxProducersPerClient(1))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).NotTo(HaveOccurred())
		_, err = env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(env.DeleteStream(streamName)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

})
