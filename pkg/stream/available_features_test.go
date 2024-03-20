package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Available Features", func() {

	It("Parse Version", func() {
		v, err := parseVersion("1.2.3")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(Version{Major: 1, Minor: 2, Patch: 3}))

		_, err = parseVersion("1.2")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid version format: 1.2"))

		_, err = parseVersion("error.3.3")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid major version: error"))

		_, err = parseVersion("1.error.3")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid minor version: error"))

		_, err = parseVersion("1.2.error")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid patch version: error"))

		v, err = parseVersion(extractVersion("3.12.1-rc1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(Version{Major: 3, Minor: 12, Patch: 1}))

		v, err = parseVersion(extractVersion("3.13.1-alpha.234"))
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(Version{Major: 3, Minor: 13, Patch: 1}))
	})

	It("Is Version Greater Or Equal", func() {
		Expect(IsVersionGreaterOrEqual("1.2.3", "1.2.3")).To(BeTrue())
		Expect(IsVersionGreaterOrEqual("1.2.3", "1.2.2")).To(BeTrue())
		Expect(IsVersionGreaterOrEqual("1.2.3", "1.2.4")).To(BeFalse())
		Expect(IsVersionGreaterOrEqual("1.2.3", "1.3.3")).To(BeFalse())
		Expect(IsVersionGreaterOrEqual("1.2.3", "2.2.3")).To(BeFalse())
		Expect(IsVersionGreaterOrEqual("3.1.3-alpha.1", "2.2.3")).To(BeFalse())
		Expect(IsVersionGreaterOrEqual("3.3.3-rc.1", "2.2.3")).To(BeFalse())

		Expect(IsVersionGreaterOrEqual("error.3.2", "2.2.3")).To(BeFalse())
		Expect(IsVersionGreaterOrEqual("4.3.2", "2.error.3")).To(BeFalse())

	})

	It("Available Features check Version", func() {
		var availableFeatures = availableFeaturesInstance()
		Expect(availableFeatures).NotTo(BeNil())
		Expect(availableFeatures.SetVersion("error")).NotTo(BeNil())
		Expect(availableFeatures.SetVersion("3.9.0")).To(BeNil())
		Expect(availableFeatures.Is311OrMore()).To(BeFalse())
		Expect(availableFeatures.Is313OrMore()).To(BeFalse())
		Expect(availableFeatures.SetVersion("3.11.0")).To(BeNil())
		Expect(availableFeatures.Is311OrMore()).To(BeTrue())
		Expect(availableFeatures.Is313OrMore()).To(BeFalse())
		Expect(availableFeatures.SetVersion("3.13.0")).To(BeNil())
		Expect(availableFeatures.Is311OrMore()).To(BeTrue())
		Expect(availableFeatures.Is313OrMore()).To(BeTrue())
		Expect(availableFeatures.SetVersion("3.13.1-alpha.234")).To(BeNil())
		Expect(availableFeatures.Is311OrMore()).To(BeTrue())
		Expect(availableFeatures.Is313OrMore()).To(BeTrue())
	})
	It("Available Features parse command", func() {
		Expect(availableFeaturesInstance().SetVersion("3.13.0")).To(BeNil())
		availableFeaturesInstance().ParseCommandVersions(
			[]commandVersion{
				PublishFilter{},
			},
		)
		Expect(availableFeaturesInstance().IsAlreadyParsed()).To(BeTrue())
		Expect(availableFeaturesInstance().BrokerFilterEnabled()).To(BeTrue())
	})
})
