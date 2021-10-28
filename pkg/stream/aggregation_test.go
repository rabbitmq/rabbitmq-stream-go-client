package stream

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("Compression algorithms", func() {

	var entries *subEntries

	BeforeEach(func() {
		messagePayload := make([]byte, 4096)
		for i := range messagePayload {
			messagePayload[i] = 99
		}

		message := messageSequence{
			messageBytes:     messagePayload,
			unCompressedSize: len(messagePayload),
			publishingId:     0,
		}

		entries = &subEntries{
			items: []*subEntry{{
				messages:         []messageSequence{message},
				publishingId:     0,
				unCompressedSize: len(messagePayload) + 4,
				sizeInBytes:      0,
				dataInBytes:      nil,
			}},
			totalSizeInBytes: 0,
		}
	})

	It("NONE", func() {
		compressNONE{}.Compress(entries)
		Expect(entries.totalSizeInBytes).To(Equal(entries.items[0].sizeInBytes))
		Expect(entries.totalSizeInBytes).To(Equal(entries.items[0].unCompressedSize))

	})

	It("GZIP", func() {
		gzip := compressGZIP{}
		gzip.Compress(entries)
		verifyCompression(gzip, entries)

	})

	It("SNAPPY", func() {
		snappy := compressSnappy{}
		snappy.Compress(entries)

		verifyCompression(snappy, entries)

	})

	It("LZ4", func() {
		lz4 := compressLZ4{}
		lz4.Compress(entries)

		verifyCompression(lz4, entries)

	})

})

func verifyCompression(algo iCompress, subEntries *subEntries) {

	Expect(subEntries.totalSizeInBytes).To(SatisfyAll(BeNumerically("<", subEntries.items[0].unCompressedSize)))
	Expect(subEntries.totalSizeInBytes).To(Equal(subEntries.items[0].sizeInBytes))

	bufferReader := bytes.NewReader(subEntries.items[0].dataInBytes)
	algo.UnCompress(bufio.NewReader(bufferReader),
		uint32(subEntries.totalSizeInBytes), uint32(subEntries.items[0].unCompressedSize))

}
