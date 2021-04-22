package streaming

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {
		It("Add/Remove Producers ", func() {
			p, err := testClient.coordinator.NewProducer(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.ID).To(Equal(uint8(0)))
			err = testClient.coordinator.RemoveProducerById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := testClient.coordinator.RemoveProducerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete coordinator ", func() {
			var producersId []uint8
			for i := 0; i < 100; i++ {
				p, err := testClient.coordinator.NewProducer(nil)
				producersId = append(producersId, p.ID)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(testClient.coordinator.ProducersCount()).To(Equal(100))
			for _, pid := range producersId {
				err := testClient.coordinator.RemoveProducerById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(testClient.coordinator.ProducersCount()).To(Equal(0))
		})

		It("To many publishers ", func() {
			var producersId []uint8
			for i := 0; i < 500; i++ {

				p, err := testClient.coordinator.NewProducer(nil)
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
				err := testClient.coordinator.RemoveProducerById(v)
				Expect(err).NotTo(HaveOccurred())
				err = testClient.coordinator.RemoveProducerById(v)
				// raise an error not found
				Expect(err).To(HaveOccurred())

				p, err := testClient.coordinator.NewProducer(nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(p.ID).To(Equal(v))
			}

			for _, pid := range producersId {
				err := testClient.coordinator.RemoveProducerById(pid)
				Expect(err).NotTo(HaveOccurred())
			}

		})

	})

	Describe("Add/Remove consumers", func() {
		It("Add/Remove consumers ", func() {
			p, err := testClient.coordinator.NewProducer(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.ID).To(Equal(uint8(0)))
			err = testClient.coordinator.RemoveProducerById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := testClient.coordinator.RemoveProducerById(200)
			Expect(err).To(HaveOccurred())
		})
		It("consumer not found get by id ", func() {
			_, err := testClient.coordinator.GetConsumerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete consumers ", func() {
			var consumersId []uint8
			for i := 0; i < 100; i++ {
				p := testClient.coordinator.NewConsumer(nil)
				consumersId = append(consumersId, p.ID)
			}
			Expect(testClient.coordinator.ConsumersCount()).To(Equal(100))
			for _, pid := range consumersId {
				err := testClient.coordinator.RemoveConsumerById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(testClient.coordinator.ConsumersCount()).To(Equal(0))
		})
	})

	Describe("Add/Remove Response", func() {
		It("Add/Remove Response ", func() {
			r := testClient.coordinator.NewResponse()
			Expect(r.correlationid).ToNot(Equal(0))
			err := testClient.coordinator.RemoveResponseById(r.correlationid)
			Expect(err).NotTo(HaveOccurred())
		})
		It("not found Response by id ", func() {
			err := testClient.coordinator.RemoveResponseById(200)
			Expect(err).To(HaveOccurred())

			err = testClient.coordinator.RemoveResponseByName("it does not exist")
			Expect(err).To(HaveOccurred())

			_, err = testClient.coordinator.GetResponseById(255)
			Expect(err).To(HaveOccurred())

		})
		It("massive insert/delete Responses ", func() {
			var responsesId []int
			for i := 0; i < 100; i++ {
				r := testClient.coordinator.NewResponse()
				responsesId = append(responsesId, r.correlationid)
			}

			for _, pid := range responsesId {
				err := testClient.coordinator.RemoveResponseById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

})
