package streaming

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {
		It("Add/Remove Producers ", func() {
			p := testClient.coordinator.NewProducer(nil)
			Expect(p.ID).To(Equal(uint8(0)))
			err := testClient.coordinator.RemoveProducerById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := testClient.coordinator.RemoveProducerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete coordinator ", func() {
			var producersId []uint8
			for i := 0; i < 100; i++ {
				p := testClient.coordinator.NewProducer(nil)
				producersId = append(producersId, p.ID)
			}
			Expect(testClient.coordinator.ProducersCount()).To(Equal(100))
			for _, pid := range producersId {
				err := testClient.coordinator.RemoveProducerById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(testClient.coordinator.ProducersCount()).To(Equal(0))
		})

	})

	Describe("Add/Remove consumers", func() {
		It("Add/Remove consumers ", func() {
			p := testClient.coordinator.NewProducer(nil)
			Expect(p.ID).To(Equal(uint8(0)))
			err := testClient.coordinator.RemoveProducerById(p.ID)
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
