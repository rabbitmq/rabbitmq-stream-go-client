package streaming

import (
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
)

const (
	UnitMb              = "mb"
	UnitKb              = "kb"
	UnitGb              = "gb"
	UnitTb              = "tb"
	kilobytesMultiplier = 1000
	megabytesMultiplier = 1000 * 1000
	gigabytesMultiplier = 1000 * 1000 * 1000
	terabytesMultiplier = 1000 * 1000 * 1000 * 1000
)

type ByteCapacity struct {
}

func (byteCapacity ByteCapacity) B(value int64) int64 {
	return value
}

func (byteCapacity ByteCapacity) KB(value int64) int64 {
	return value * kilobytesMultiplier
}

func (byteCapacity ByteCapacity) MB(value int64) int64 {
	return value * megabytesMultiplier
}

func (byteCapacity ByteCapacity) GB(value int64) int64 {
	return value * gigabytesMultiplier
}
func (byteCapacity ByteCapacity) TB(value int64) int64 {
	return value * terabytesMultiplier
}

func (byteCapacity ByteCapacity) From(value string) (int64, error) {

	match, err := regexp.Compile("^((kb|mb|gb|tb))")
	if err != nil {
		return 0, errors.New("Invalid unit size format")
	}

	foundUnitSize := strings.ToLower(value[len(value)-2:])

	if match.MatchString(foundUnitSize) {

		size, err := strconv.Atoi(value[:len(value)-2])
		if err != nil {
			return 0, errors.New("Invalid number format")
		}

		switch foundUnitSize {
		case UnitKb:
			return byteCapacity.KB(int64(size)), nil

		case UnitMb:
			return byteCapacity.MB(int64(size)), nil

		case UnitGb:
			return byteCapacity.GB(int64(size)), nil

		case UnitTb:
			return byteCapacity.TB(int64(size)), nil
		}

	}

	return 0, errors.New("Invalid unit size format")

}
