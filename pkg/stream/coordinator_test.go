package stream

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = BeforeSuite(func() {
	InitCoordinators()
})

var _ = AfterSuite(func() {
	Expect(GetProducers().Count()).To(Equal(0))

})

var _ = Describe("Coordinator", func() {
	BeforeEach(func() {

	})
	AfterEach(func() {
	})

	Describe("Add/Remove Producers", func() {
		It("Add producer ", func() {
			p := GetProducers().New()
			Expect(p.ProducerID).To(Equal(uint8(0)))
		})
		It("Get producer by id ", func() {
			p, err := GetProducers().GetById(uint8(0))
			Expect(err).NotTo(HaveOccurred())
			Expect(p.ProducerID).To(Equal(uint8(0)))
		})
		It("Remove producer by id ", func() {
			err := GetProducers().RemoveById(0)
			Expect(err).NotTo(HaveOccurred())
			Expect(GetProducers().Count()).To(Equal(0))
		})
		It("not found producer by id ", func() {
			err := GetProducers().RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
		It("massive insert/delete producers ", func() {
			var producersId []uint8
			for i := 0; i < 100; i++ {
				p := GetProducers().New()
				producersId = append(producersId, p.ProducerID)
			}
			Expect(GetProducers().Count()).To(Equal(100))
			for _, pid := range producersId {
				err := GetProducers().RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(GetProducers().Count()).To(Equal(0))
		})
	})

	Describe("Add/Remove Response", func() {
		It("Add Response ", func() {
			r := GetResponses().New()
			Expect(r.subId).To(Equal(0))
		})
		It("Get Response by id ", func() {
			p, err := GetResponses().GetById(0)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.subId).To(Equal(0))
		})
		It("Remove Response by id ", func() {
			err := GetResponses().RemoveById(0)
			Expect(err).NotTo(HaveOccurred())
			Expect(GetResponses().Count()).To(Equal(0))
		})
		It("not found producer by id ", func() {
			err := GetResponses().RemoveById(200)
			Expect(err).To(HaveOccurred())
		})
		It("massive insert/delete Responses ", func() {
			var responsesId []int
			for i := 0; i < 100; i++ {
				r := GetResponses().New()
				responsesId = append(responsesId, r.subId)
			}
			Expect(GetResponses().Count()).To(Equal(100))
			for _, pid := range responsesId {
				err := GetResponses().RemoveById(pid)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(GetResponses().Count()).To(Equal(0))
		})
	})

})
