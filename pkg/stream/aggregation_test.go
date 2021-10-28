package stream

import (
	"bufio"
	"bytes"
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

		Expect(subEntries.totalSizeInBytes).To(Equal(subEntries.items[0].sizeInBytes))
		Expect(subEntries.totalSizeInBytes).To(Equal(subEntries.items[0].unCompressedSize))

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
		gzip := compressGZIP{}
		gzip.Compress(subEntries)

		verifyCompression(gzip, subEntries)

	})


	It("SNAPPY", func() {
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

		gzip := compressSnappy{}
		gzip.Compress(subEntries)

		verifyCompression(gzip, subEntries)

	})

})
func verifyCompression(algo iCompress, subEntries *subEntries) {

	Expect(subEntries.totalSizeInBytes).To(SatisfyAll(BeNumerically("<", subEntries.items[0].unCompressedSize)))
	Expect(subEntries.totalSizeInBytes).To(Equal(subEntries.items[0].sizeInBytes))

	bufferReader := bytes.NewReader(subEntries.items[0].dataInBytes)
	algo.UnCompress(bufio.NewReader(bufferReader),
		uint32(subEntries.totalSizeInBytes), uint32(subEntries.items[0].unCompressedSize))

}