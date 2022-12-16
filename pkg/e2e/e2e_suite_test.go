//go:build rabbitmq.stream.e2e

package e2e_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"os/exec"
	"testing"
	"time"
)

func TestE2e(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

var _ = SynchronizedBeforeSuite(func(ctx SpecContext) {
	startCommand := exec.Command("../../scripts/start-docker.bash")
	session, err := gexec.Start(startCommand, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())

	// long timeout because docker pull may take a while
	Eventually(session).
		WithTimeout(time.Second*20).
		WithPolling(time.Second).
		Should(gexec.Exit(0), "expected rabbit container to be started")

	// Defer container stop == docker stop <rabbitmq-container-id>
	DeferCleanup(func() {
		stopCommand := exec.Command("../../scripts/stop-docker.bash")
		session, err := gexec.Start(stopCommand, GinkgoWriter, GinkgoWriter)
		Expect(err).ToNot(HaveOccurred())
		Eventually(session).
			WithTimeout(time.Second*20).
			WithPolling(time.Second).
			Should(gexec.Exit(0), "expected rabbit container to stop")
	})

	Eventually(func(g Gomega) {
		readyCmd := exec.Command("../../scripts/readiness-check.bash")
		session, err = gexec.Start(readyCmd, GinkgoWriter, GinkgoWriter)
		g.Expect(err).ToNot(HaveOccurred())
		session.Wait(time.Second * 5)
		g.Expect(session).Should(gexec.Exit(0), "expected rabbitmq to be ready on port 5552")
	}).
		WithTimeout(time.Second * 15).
		WithPolling(time.Second).
		Should(Succeed())
}, func() {})
