package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
)

var _ = FDescribe("Compression algorithms", func() {

	It("NONE", func() {
		messagePayload := make([]byte, 4096)
		n, err := rand.Read(messagePayload)
		fmt.Printf("n: %d , err:%v", n, err)

		message := messageSequence{
			messageBytes:     messagePayload,
			unCompressedSize: len(messagePayload),
			publishingId:     0,
		}

		subEntries := &subEntries{
			items: []*subEntry{{
				messages:         []messageSequence{message},
				publishingId:     0,
				unCompressedSize: len(messagePayload) + 4,
				sizeInBytes:      0,
				dataInBytes:      nil,
			}},
			totalSizeInBytes: 0,
		}
		compressNONE{}.Compress(subEntries)

		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", len(messagePayload))))
		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", subEntries.items[0].unCompressedSize)))
		//Expect(subEntries.totalSizeInBytes).To(SatisfyAll(BeNumerically("<", )))
	})

	It("GZIP", func() {
		messagePayload := make([]byte, 4096)
		n, err := rand.Read(messagePayload)
		for i := range messagePayload {
			messagePayload[i] = 1
		}
		fmt.Printf("n: %d , err:%v", n, err)

		message := messageSequence{
			messageBytes:     messagePayload,
			unCompressedSize: len(messagePayload),
			publishingId:     0,
		}

		subEntries := &subEntries{
			items: []*subEntry{{
				messages:         []messageSequence{message},
				publishingId:     0,
				unCompressedSize: len(messagePayload) + 4,
				sizeInBytes:      0,
				dataInBytes:      nil,
			}},
			totalSizeInBytes: 0,
		}
		compressGZIP{}.Compress(subEntries)

		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", len(messagePayload))))
		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", subEntries.items[0].unCompressedSize)))
		//Expect(subEntries.totalSizeInBytes).To(SatisfyAll(BeNumerically("<", )))
	})

	FIt("SNAPPY", func() {
		messagePayload := make([]byte, 4096)
		for i := range messagePayload {
			messagePayload[i] = 1
		}

		message := messageSequence{
			messageBytes:     messagePayload,
			unCompressedSize: len(messagePayload),
			publishingId:     0,
		}

		subEntries := &subEntries{
			items: []*subEntry{{
				messages:         []messageSequence{message},
				publishingId:     0,
				unCompressedSize: len(messagePayload) + 4,
				sizeInBytes:      0,
				dataInBytes:      nil,
			}},
			totalSizeInBytes: 0,
		}
		compressSnappy{}.Compress(subEntries)

		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", len(messagePayload))))
		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", subEntries.items[0].unCompressedSize)))

	})

})
