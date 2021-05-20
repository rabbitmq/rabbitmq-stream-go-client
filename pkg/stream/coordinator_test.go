package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var client = newClient("test-client")
var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {
		It("Add/Remove Producers ", func() {
			p, err := client.coordinator.NewProducer(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.ID).To(Equal(uint8(0)))
			err = client.coordinator.RemoveProducerById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := client.coordinator.RemoveProducerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete coordinator ", func() {
			var producersId []uint8
			for i := 0; i < 100; i++ {
				p, err := client.coordinator.NewProducer(nil, nil)
				producersId = append(producersId, p.ID)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.coordinator.ProducersCount()).To(Equal(100))
			for _, pid := range producersId {
				err := client.coordinator.RemoveProducerById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.coordinator.ProducersCount()).To(Equal(0))
		})

		It("To many publishers ", func() {
			var producersId []uint8
			for i := 0; i < 500; i++ {

				p, err := client.coordinator.NewProducer(nil, nil)
				if i >= int(^uint8(0)) {
					Expect(fmt.Sprintf("%s", err)).
						To(ContainSubstring("No more items available"))
				} else {
					producersId = append(producersId, p.ID)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			// just some random remove,
			randomRemove := []uint8{5, 127, 250, 36, 57, 99, 102, 88}
			for _, v := range randomRemove {
				// remove an producer then recreate it and I must have the
				// missing item
				err := client.coordinator.RemoveProducerById(v)
				Expect(err).NotTo(HaveOccurred())
				err = client.coordinator.RemoveProducerById(v)
				// raise an logError not found
				Expect(err).To(HaveOccurred())

				p, err := client.coordinator.NewProducer(nil, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(p.ID).To(Equal(v))
			}

			for _, pid := range producersId {
				err := client.coordinator.RemoveProducerById(pid)
				Expect(err).NotTo(HaveOccurred())
			}

		})

	})

	Describe("Add/Remove consumers", func() {
		It("Add/Remove consumers ", func() {
			p, err := client.coordinator.NewProducer(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.ID).To(Equal(uint8(0)))
			err = client.coordinator.RemoveProducerById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := client.coordinator.RemoveProducerById(200)
			Expect(err).To(HaveOccurred())
		})
		It("consumer not found get by id ", func() {
			_, err := client.coordinator.GetConsumerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete consumers ", func() {
			var consumersId []uint8
			for i := 0; i < 100; i++ {
				p := client.coordinator.NewConsumer(nil, nil, nil)
				consumersId = append(consumersId, p.ID)
			}
			Expect(client.coordinator.ConsumersCount()).To(Equal(100))
			for _, pid := range consumersId {
				err := client.coordinator.RemoveConsumerById(pid, Event{
					Command:    0,
					StreamName: "UNIT_TESTS",
					Name:       "",
					Err:        nil,
				})
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.coordinator.ConsumersCount()).To(Equal(0))
		})
	})

	Describe("Add/Remove Response", func() {
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
			for i := 0; i < 100; i++ {
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
