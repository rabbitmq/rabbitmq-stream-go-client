package ha_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHa(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ha Suite")
}
