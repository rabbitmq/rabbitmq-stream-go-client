package streaming

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {
		It("Add/Remove Producers ", func() {
			p := testClient.producers.NewProducer(nil)
			Expect(p.ID).To(Equal(uint8(0)))
			err := testClient.producers.RemoveById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("producer not found remove by id ", func() {
			err := testClient.producers.RemoveById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete producers ", func() {
			var producersId []uint8
			for i := 0; i < 100; i++ {
				p := testClient.producers.NewProducer(nil)
				producersId = append(producersId, p.ID)
			}
			Expect(testClient.producers.Count()).To(Equal(100))
			for _, pid := range producersId {
				err := testClient.producers.RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(testClient.producers.Count()).To(Equal(0))
		})

	})

	Describe("Add/Remove consumers", func() {
		It("Add/Remove consumers ", func() {
			p := testClient.consumers.NewProducer(nil)
			Expect(p.ID).To(Equal(uint8(0)))
			err := testClient.consumers.RemoveById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("consumer not found remove by id ", func() {
			err := testClient.consumers.RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
		It("consumer not found get by id ", func() {
			_, err := testClient.consumers.GetConsumerById(200)
			Expect(err).To(HaveOccurred())
		})

		It("massive insert/delete consumers ", func() {
			var consumersId []uint8
			for i := 0; i < 100; i++ {
				p := testClient.consumers.NewProducer(nil)
				consumersId = append(consumersId, p.ID)
			}
			Expect(testClient.consumers.Count()).To(Equal(100))
			for _, pid := range consumersId {
				err := testClient.consumers.RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(testClient.consumers.Count()).To(Equal(0))
		})
	})

	Describe("Add/Remove Response", func() {
		It("Add/Remove Response ", func() {
			r := testClient.responses.NewResponse()
			Expect(r.subId).ToNot(Equal(0))
			err := testClient.responses.RemoveResponseById(r.subId)
			Expect(err).NotTo(HaveOccurred())
		})
		It("not found Response by id ", func() {
			err := testClient.responses.RemoveResponseById(200)
			Expect(err).To(HaveOccurred())
		})
		It("massive insert/delete Responses ", func() {
			var responsesId []int
			for i := 0; i < 100; i++ {
				r := testClient.responses.NewResponse()
				responsesId = append(responsesId, r.subId)
			}

			for _, pid := range responsesId {
				err := testClient.responses.RemoveResponseById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

})
