//go:build rabbitmq.stream.e2e

package e2e_test

import (
	"flag"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"os"
	"os/exec"
	"testing"
	"time"
)

var keepRabbitContainer, rabbitDebugLog bool

const defaultContainerName = "rabbitmq-stream-client-test"

func init() {
	flag.BoolVar(&keepRabbitContainer, "keep-rabbit-container", false, "Keep RabbitMQ container after suite run")
	flag.BoolVar(&rabbitDebugLog, "rabbit-debug-log", false, "Configure debug log level in RabbitMQ")
}

func TestE2e(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

var _ = SynchronizedBeforeSuite(func(ctx SpecContext) {
	skipContainerStart := os.Getenv("RMQ_E2E_SKIP_CONTAINER_START")
	if skipContainerStart == "" {
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
			var cmdArgs []string
			if keepRabbitContainer {
				cmdArgs = []string{"-p"}
			}
			stopCommand := exec.Command("../../scripts/stop-docker.bash", cmdArgs...)
			session, err := gexec.Start(stopCommand, GinkgoWriter, GinkgoWriter)
			Expect(err).ToNot(HaveOccurred())
			Eventually(session).
				WithTimeout(time.Second*20).
				WithPolling(time.Second).
				Should(gexec.Exit(0), "expected rabbit container to stop")
		})

		Eventually(func(g Gomega) {
			readyCmd := exec.Command("../../scripts/readiness-check.bash")
			session, err := gexec.Start(readyCmd, GinkgoWriter, GinkgoWriter)
			g.Expect(err).ToNot(HaveOccurred())
			session.Wait(time.Second * 5)
			g.Expect(session).Should(gexec.Exit(0), "expected rabbitmq to be ready on port 5552")
		}).
			WithTimeout(time.Second * 15).
			WithPolling(time.Second * 5).
			Should(Succeed())
	}

	containerName, found := os.LookupEnv("RABBITMQ_CONTAINER_NAME")
	if !found {
		containerName = defaultContainerName
	}

	pluginsCmd := exec.Command(
		"docker",
		"exec",
		"--user=rabbitmq",
		containerName,
		"rabbitmq-plugins",
		"enable",
		"rabbitmq_stream",
		"rabbitmq_stream_management",
		"rabbitmq_management",
	)
	session, err := gexec.Start(pluginsCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	Eventually(session).
		WithTimeout(time.Second*15).
		WithPolling(time.Second).
		Should(gexec.Exit(0), "expected to enable stream plugin")

	if rabbitDebugLog {
		debugCmd := exec.Command(
			"docker",
			"exec",
			"--user=rabbitmq",
			containerName,
			"rabbitmqctl",
			"set_log_level",
			"debug",
		)
		session, err := gexec.Start(debugCmd, GinkgoWriter, GinkgoWriter)
		Expect(err).ToNot(HaveOccurred())
		Eventually(session).
			WithTimeout(time.Second*5).
			WithPolling(time.Second).
			Should(gexec.Exit(0), "expected to enable rabbitmq debug logs")
	}
}, func() {})
