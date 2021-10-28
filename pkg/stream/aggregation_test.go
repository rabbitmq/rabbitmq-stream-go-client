package stream

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"math/rand"
)

var _ = Describe("Compression algorithms", func() {

	FIt("SNAPPY", func() {
		messagePayload := make([]byte, 1024)
		rand.Read(messagePayload)

		message := messageSequence{
			messageBytes:     messagePayload,
			unCompressedSize: len(messagePayload),
			publishingId:     0,
		}

		subEntries := &subEntries{
			items: []*subEntry{{
				messages:         []messageSequence{message},
				publishingId:     0,
				unCompressedSize: len(messagePayload),
				sizeInBytes:      len(messagePayload),
				dataInBytes:      message.messageBytes,
			}},
			totalSizeInBytes: 10,
		}
		compressSnappy{}.Compress(subEntries)

		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", subEntries.items[0].unCompressedSize)))
		Expect(subEntries.items[0].sizeInBytes).To(SatisfyAll(BeNumerically("<", len(messagePayload))))

	})

})
