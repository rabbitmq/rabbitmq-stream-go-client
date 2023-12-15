package stream_test

import (
	"bytes"
	"encoding/binary"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stream Suite")
}

const (
	SystemTestEnvVarName        = "RABBITMQ_STREAM_RUN_SYSTEM_TEST"
	containerName        string = "rabbitmq-stream-go-client"
)

type plainTextMessage struct {
	body string
}

func (p *plainTextMessage) UnmarshalBinary(data []byte) error {
	var dataLen uint32
	err := binary.Read(bytes.NewReader(data), binary.BigEndian, &dataLen)
	if err != nil {
		return err
	}

	p.body = string(data[4 : dataLen+4])

	return nil
}

func (p *plainTextMessage) MarshalBinary() ([]byte, error) {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, uint32(len(p.body)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, []byte(p.body))
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (p *plainTextMessage) Body() string {
	return p.body
}

var _ = SynchronizedBeforeSuite(func() {
	// Just once
	logger := log.New(GinkgoWriter, "[SBS] ", log.Ldate|log.Lmsgprefix)
	if _, isSet := os.LookupEnv(SystemTestEnvVarName); !isSet {
		logger.Println("System test variable to run system test not set. Skipping system tests...")
		return
	}

	dockerRunArgs := strings.Split(
		"run --rm --detach --name "+containerName+" "+
			"-p 5672:5672 -p 15672:15672 -p 5552:5552 "+
			"rabbitmq:3-management",
		" ")
	cmd := exec.Command("docker", dockerRunArgs...)
	session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	session.Wait()

	Eventually(func() *gbytes.Buffer {
		cmd := exec.Command("docker", "exec", containerName, "epmd", "-names")
		bufErr := gbytes.NewBuffer()
		session, _ := gexec.Start(cmd, bufErr, bufErr)
		session.Wait()
		return bufErr
	}).WithPolling(time.Second).WithTimeout(time.Second*60).
		Should(gbytes.Say("rabbit"), "expected epmd to report rabbit app as running")

	awaitStartArgs := strings.Split("exec --user rabbitmq -i "+containerName+" rabbitmqctl await_startup", " ")
	awaitCmd := exec.Command("docker", awaitStartArgs...)
	awaitSession, err := gexec.Start(awaitCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	awaitSession.Wait(time.Second * 60)

	enablePluginArgs := strings.Split("exec --user rabbitmq -i "+containerName+" rabbitmq-plugins enable rabbitmq_stream", " ")
	enablePluginCmd := exec.Command("docker", enablePluginArgs...)
	enablePluginSession, err := gexec.Start(enablePluginCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	enablePluginSession.Wait(time.Second * 60)
}, func() {
	// All processes
})

var _ = SynchronizedAfterSuite(func() {
	// all processes
}, func() {
	// Just once
	logger := log.New(GinkgoWriter, "[SAS] ", log.Lmsgprefix|log.Ldate)
	if _, isSet := os.LookupEnv("RABBITMQ_STREAM_KEEP_CONTAINER"); isSet {
		logger.Println("Keep container env variable set. RabbitMQ container won't be stopped")
		return
	}
	stopArgs := strings.Split("stop "+containerName, " ")
	stopCmd := exec.Command("docker", stopArgs...)
	session, err := gexec.Start(stopCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	session.Wait(time.Second * 15)
})
