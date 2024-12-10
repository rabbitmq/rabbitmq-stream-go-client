package stream

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Describe("UnConfirmed tests ", func() {

	It("Hash Consistency", func() {
		unConfirmed := newUnConfirmed(10)
		for i := 0; i < 10; i++ {
			unConfirmed.add(int64(i), &ConfirmationStatus{})
		}
		for i := 0; i < 10; i++ {
			Expect(unConfirmed.get(int64(i))).NotTo(BeNil())
		}
		Expect(len(unConfirmed.partitions)).To(Equal(10))
		Expect(unConfirmed.partitions[0].size()).To(Equal(1))
		Expect(unConfirmed.partitions[1].size()).To(Equal(1))
		Expect(unConfirmed.partitions[2].size()).To(Equal(1))
		Expect(unConfirmed.partitions[3].size()).To(Equal(1))
		Expect(unConfirmed.partitions[4].size()).To(Equal(1))
		Expect(unConfirmed.partitions[5].size()).To(Equal(1))
		Expect(unConfirmed.partitions[6].size()).To(Equal(1))
		Expect(unConfirmed.partitions[7].size()).To(Equal(1))
		Expect(unConfirmed.partitions[8].size()).To(Equal(1))
		Expect(unConfirmed.partitions[9].size()).To(Equal(1))
		unConfirmed.clear()
		for i := 0; i < 10; i++ {
			Expect(unConfirmed.partitions[i].size()).To(Equal(0))
		}
		Expect(unConfirmed.size()).To(Equal(0))
	})

	It("GetAll Result should be consistent", func() {
		// the map should be order
		// even it is not strictly necessary to be order

		for sz := 1; sz <= 10; sz++ {
			unConfirmed := newUnConfirmed(sz)
			for i := 0; i < 500; i++ {
				unConfirmed.add(int64(i), &ConfirmationStatus{})
			}
			result := unConfirmed.getAll()
			exceptedValue := 0
			for i, status := range result {
				Expect(i).To(Equal(status.GetPublishingId()))
				Expect(i).To(Equal(int64(exceptedValue)))
				exceptedValue++
			}
		}

	})

	It("GetAll Result should be consistent in multi-thread", func() {
		// the map should be order in multi-thread
		// even it is not strictly necessary to be order

		for sz := 1; sz <= 10; sz++ {
			unConfirmed := newUnConfirmed(sz)
			wait := &sync.WaitGroup{}
			for i := 0; i < 500; i++ {
				wait.Add(1)
				go func(idx int) {
					unConfirmed.add(int64(idx), &ConfirmationStatus{})
					wait.Done()
				}(i)
			}
			wait.Wait()

			result := unConfirmed.getAll()
			exceptedValue := 0
			for i, status := range result {
				Expect(i).To(Equal(status.GetPublishingId()))
				Expect(i).To(Equal(int64(exceptedValue)))
				exceptedValue++
			}
		}

	})

})
