package stream



//var _ = Describe("Coordinator", func() {
//	BeforeEach(func() {
//
//	})
//	AfterEach(func() {
//	})
//
//	Describe("Add/Remove Producers", func() {
//		It("Add producer ", func() {
//			p := client.producers.New()
//			Expect(p.ProducerID).To(Equal(uint8(0)))
//		})
//		It("Get producer by id ", func() {
//			p, err := client.producers.GetById(uint8(0))
//			Expect(err).NotTo(HaveOccurred())
//			Expect(p.ProducerID).To(Equal(uint8(0)))
//		})
//		It("Remove producer by id ", func() {
//			err := client.producers.RemoveById(0)
//			Expect(err).NotTo(HaveOccurred())
//			Expect(client.producers.Count()).To(Equal(0))
//		})
//		It("not found producer by id ", func() {
//			err := client.producers.RemoveById(200)
//			Expect(err).To(HaveOccurred())
//		})
//		It("massive insert/delete producers ", func() {
//			var producersId []uint8
//			for i := 0; i < 100; i++ {
//				p := client.producers.New()
//				producersId = append(producersId, p.ProducerID)
//			}
//			Expect(client.producers.Count()).To(Equal(100))
//			for _, pid := range producersId {
//				err := client.producers.RemoveById(pid)
//				Expect(err).NotTo(HaveOccurred())
//			}
//			Expect(client.producers.Count()).To(Equal(0))
//		})
//	})
//
//	Describe("Add/Remove Response", func() {
//		It("Add Response ", func() {
//			r := client.responses.New()
//			Expect(r.subId).ToNot(Equal(0))
//		})
//		It("Get Response by id ", func() {
//			p, err := client.responses.GetById(0)
//			Expect(err).NotTo(HaveOccurred())
//			Expect(p.subId).ToNot(Equal(0))
//		})
//		It("Remove Response by id ", func() {
//			err := client.responses.RemoveById(0)
//			Expect(err).NotTo(HaveOccurred())
//			Expect(client.responses.Count()).To(Equal(0))
//		})
//		It("not found producer by id ", func() {
//			err := client.responses.RemoveById(200)
//			Expect(err).To(HaveOccurred())
//		})
//		It("massive insert/delete Responses ", func() {
//			var responsesId []int
//			for i := 0; i < 100; i++ {
//				r := client.responses.New()
//				responsesId = append(responsesId, r.subId)
//			}
//			Expect(client.responses.Count()).To(Equal(100))
//			for _, pid := range responsesId {
//				err := client.responses.RemoveById(pid)
//				Expect(err).NotTo(HaveOccurred())
//			}
//			Expect(client.responses.Count()).To(Equal(0))
//		})
//	})
//
//})
