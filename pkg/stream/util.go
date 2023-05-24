package stream

import (
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
)

const (
	maxAgeKey         = "x-max-age"
	maxLengthKey      = "x-max-length-bytes"
	maxSegmentSizeKey = "x-stream-max-segment-size-bytes"
)

func streamOptionsToRawStreamConfiguration(options StreamOptions) raw.StreamConfiguration {
	c := make(raw.StreamConfiguration, 3)
	if options.MaxLength != 0 {
		c[maxLengthKey] = options.MaxLength.String()
	}

	if options.MaxSegmentSize != 0 {
		c[maxSegmentSizeKey] = options.MaxSegmentSize.String()
	}

	if options.MaxAge > 0 {
		c[maxAgeKey] = fmt.Sprintf("%.0fs", options.MaxAge.Seconds())
	}

	return c
}
