package stream

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {

		var (
			client *Client
		)
		BeforeEach(func() {
			client = newClient(clientConnectionParameters{
				connectionName: "test-client",
				rpcTimeOut:     defaultSocketCallTimeout,
			})

		})
		AfterEach(func() {
			client = nil

		})

		It("Add/Remove Producers ", func() {
			p, err := client.coordinator.NewProducer(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.id).To(Equal(uint8(0)))
			err = client.coordinator.RemoveProducerById(p.id, Event{
				Command: 0,
				Reason:  "UNIT_TEST",
				Err:     nil,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := client.coordinator.RemoveProducerById(200, Event{
				Command: 0,
				Reason:  "UNIT_TEST",
				Err:     nil,
			})
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete coordinator ", func() {
			var producersId []uint8
			for range 100 {
				p, err := client.coordinator.NewProducer(nil, nil)
				producersId = append(producersId, p.id)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.coordinator.ProducersCount()).To(Equal(100))
			for _, pid := range producersId {
				err := client.coordinator.RemoveProducerById(pid, Event{
					Command: 0,
					Reason:  "UNIT_TEST",
					Err:     nil,
				})
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.coordinator.ProducersCount()).To(Equal(0))
		})

		It("Get next publisher id ", func() {
			// until reach 255 then start reusing the old
			// unused ids
			for i := 0; i < 250; i++ {
				p, err := client.coordinator.NewProducer(nil, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(p.id).To(Equal(uint8(i)))
				err = client.coordinator.RemoveProducerById(p.id, Event{
					Command:    0,
					StreamName: "",
					Name:       "UNIT TEST",
					Reason:     "",
					Err:        nil,
				})
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("To many publishers ", func() {
			var producersId []uint8
			for i := 0; i < 500; i++ {

				p, err := client.coordinator.NewProducer(nil, nil)
				if i >= int(^uint8(0)) {
					Expect(fmt.Sprintf("%s", err)).
						To(ContainSubstring("No more items available"))
				} else {
					Expect(err).NotTo(HaveOccurred())
					producersId = append(producersId, p.id)
				}
			}

			// just some random remove,
			randomRemove := []uint8{5, 127, 250, 36, 57, 99, 102, 88}
			for _, v := range randomRemove {
				// remove an producer then recreate it and I must have the
				// missing item
				err := client.coordinator.RemoveProducerById(v, Event{
					Reason: "UNIT_TEST",
				})
				Expect(err).NotTo(HaveOccurred())
				err = client.coordinator.RemoveProducerById(v, Event{
					Reason: "UNIT_TEST",
				})
				// raise an logError not found
				Expect(err).To(HaveOccurred())

				p, err := client.coordinator.NewProducer(nil, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(p.id).To(Equal(v))
			}

			for _, pid := range producersId {
				err := client.coordinator.RemoveProducerById(pid, Event{
					Reason: "UNIT_TEST",
				})
				Expect(err).NotTo(HaveOccurred())
			}

		})

	})

	Describe("Add/Remove consumers", func() {

		var (
			client *Client
		)
		BeforeEach(func() {
			client = newClient(clientConnectionParameters{
				connectionName: "test-client",
				rpcTimeOut:     defaultSocketCallTimeout,
			})

		})
		AfterEach(func() {
			client = nil

		})

		It("Add/Remove consumers ", func() {
			p, err := client.coordinator.NewProducer(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.id).To(Equal(uint8(0)))
			err = client.coordinator.RemoveProducerById(p.id, Event{
				Reason: "UNIT_TEST",
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := client.coordinator.RemoveProducerById(200, Event{
				Reason: "UNIT_TEST",
			})
			Expect(err).To(HaveOccurred())
		})
		It("consumer not found get by id ", func() {
			_, err := client.coordinator.GetConsumerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete consumers ", func() {
			var consumersId []uint8
			for range 100 {
				p := client.coordinator.NewConsumer(nil, NewConsumerOptions(), nil)
				consumersId = append(consumersId, p.id)
			}
			Expect(client.coordinator.ConsumersCount()).To(Equal(100))
			for _, pid := range consumersId {
				err := client.coordinator.RemoveConsumerById(pid, Event{
					Command:    0,
					StreamName: "UNIT_TESTS",
					Name:       "",
					Reason:     "UNIT_TEST",
					Err:        nil,
				})
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.coordinator.ConsumersCount()).To(Equal(0))
		})
	})

	Describe("Add/Remove Response", func() {
		var (
			client *Client
		)
		BeforeEach(func() {
			client = newClient(clientConnectionParameters{
				connectionName: "test-client",
				rpcTimeOut:     defaultSocketCallTimeout,
			})

		})
		AfterEach(func() {
			client = nil

		})

		It("Add/Remove Response ", func() {
			r := client.coordinator.NewResponse(commandUnitTest)
			Expect(r.correlationid).ToNot(Equal(0))
			err := client.coordinator.RemoveResponseById(r.correlationid)
			Expect(err).NotTo(HaveOccurred())
		})
		It("not found Response by id ", func() {
			err := client.coordinator.RemoveResponseById(200)
			Expect(err).To(HaveOccurred())

			err = client.coordinator.RemoveResponseByName("it does not exist")
			Expect(err).To(HaveOccurred())

			_, err = client.coordinator.GetResponseById(255)
			Expect(err).To(HaveOccurred())

		})
		It("massive insert/delete Responses ", func() {
			var responsesId []int
			for range 100 {
				r := client.coordinator.NewResponse(commandUnitTest)
				responsesId = append(responsesId, r.correlationid)
			}

			for _, pid := range responsesId {
				err := client.coordinator.RemoveResponseById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

})
