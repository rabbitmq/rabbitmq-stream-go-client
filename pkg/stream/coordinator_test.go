package stream
import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)


var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {
		It("Add/Remove Producers ", func() {

			p := client.producers.New(client)
			Expect(p.ID).To(Equal(uint8(0)))
			err := client.producers.RemoveById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("not found Producers by id ", func() {
			err := client.producers.RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
		It("massive insert/delete producers ", func() {
			var producersId []uint8
			for i := 0; i < 100; i++ {
				p := client.producers.New(client)
				producersId = append(producersId, p.ID)
			}
			Expect(client.producers.Count()).To(Equal(100))
			for _, pid := range producersId {
				err := client.producers.RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.producers.Count()).To(Equal(0))
		})
	})

	Describe("Add/Remove consumers", func() {
		It("Add/Remove consumers ", func() {
			p := client.consumers.New(client)
			Expect(p.ID).To(Equal(uint8(0)))
			err := client.consumers.RemoveById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("not found consumers by id ", func() {
			err := client.consumers.RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
		It("massive insert/delete consumers ", func() {
			var consumersId []uint8
			for i := 0; i < 100; i++ {
				p := client.consumers.New(client)
				consumersId = append(consumersId, p.ID)
			}
			Expect(client.consumers.Count()).To(Equal(100))
			for _, pid := range consumersId {
				err := client.consumers.RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(client.consumers.Count()).To(Equal(0))
		})
	})

	Describe("Add/Remove Response", func() {
		It("Add/Remove Response ", func() {
			r := client.responses.New()
			Expect(r.SubId).ToNot(Equal(0))
			err := client.responses.RemoveById(r.SubId)
			Expect(err).NotTo(HaveOccurred())
		})
		It("not found Response by id ", func() {
			err := client.responses.RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
			It("massive insert/delete Responses ", func() {
			var responsesId []int
			for i := 0; i < 100; i++ {
				r := client.responses.New()
				responsesId = append(responsesId, r.SubId)
			}

			for _, pid := range responsesId {
				err := client.responses.RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

})
