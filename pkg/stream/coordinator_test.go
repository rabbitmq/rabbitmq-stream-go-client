package stream

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Coordinator", func() {

	Describe("Add/Remove Producers", func() {

		var (
		//client *Client
		)
		BeforeEach(func() {
			//client = newClient("test-client", nil, nil)

		})
		AfterEach(func() {
			//client = nil

		})

	})

	Describe("Add/Remove consumers", func() {

		var (
			client *Client
		)
		BeforeEach(func() {
			client = newClient("test-client", nil, nil)

		})
		AfterEach(func() {
			client = nil

		})

		It("massive insert/delete consumers ", func() {
			var consumersId []uint8
			for i := 0; i < 100; i++ {
				p := client.coordinator.NewConsumer(nil, nil)
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
		var (
			client *Client
		)
		BeforeEach(func() {
			client = newClient("test-client", nil, nil)

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
