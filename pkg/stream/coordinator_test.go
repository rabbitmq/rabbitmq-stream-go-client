package stream
import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)


var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {
		It("Add/Remove subscribe ", func() {
			p := client.producers.New(client)
			Expect(p.ID).To(Equal(uint8(0)))
			err := client.producers.RemoveById(p.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("not found subscribe by id ", func() {
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

	Describe("Add/Remove Response", func() {
		It("Add/Remove Response ", func() {
			r := client.responses.New()
			Expect(r.subId).ToNot(Equal(0))
			err := client.responses.RemoveById(r.subId)
			Expect(err).NotTo(HaveOccurred())
		})
		It("not found subscribe by id ", func() {
			err := client.responses.RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
		It("massive insert/delete Responses ", func() {
			var responsesId []int
			for i := 0; i < 100; i++ {
				r := client.responses.New()
				responsesId = append(responsesId, r.subId)
			}
			// one client.responses is allocated by the heartbeat
			// this is why 100 + 1
			Expect(client.responses.Count()).To(Equal(101))
			for _, pid := range responsesId {
				err := client.responses.RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			//one client.responses is allocated by the heartbeat
			Expect(client.responses.Count()).To(Equal(1))
		})
	})

})
