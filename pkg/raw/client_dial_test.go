package raw_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
)

var _ = Describe("ClientDial", func() {

	var (
		fakeServer       *fakeRabbitMQServer
		tempLocation     string
		serverSocketPath string
		waiter           chan struct{}
	)

	BeforeEach(func() {
		waiter = make(chan struct{})
		var err error
		tempLocation, err = os.MkdirTemp("", "rabbitmq-stream-go-client-test")
		Expect(err).ToNot(HaveOccurred())
		serverSocketPath = filepath.Join(tempLocation, "server.sock")
		go func() {
			defer GinkgoRecover()
			l, err := net.Listen("unix", serverSocketPath)
			Expect(err).ToNot(HaveOccurred())

			conn, err := l.Accept()
			Expect(err).ToNot(HaveOccurred())

			fakeServer = &fakeRabbitMQServer{
				correlationIdSeq: autoIncrementingSequence{0},
				connection:       conn,
				deadlineDelta:    time.Second,
				done:             make(chan struct{}, 1),
			}

			close(waiter)

			_ = l.Close()
		}()
	})

	It("receives a connection closed notification", func(ctx SpecContext) {
		<-time.After(time.Millisecond * 100) // have to introduce artificial delay for the background frame handler

		conf, _ := raw.NewClientConfiguration("")
		conf.SetDial(func(_, _ string) (net.Conn, error) {
			c, err := net.DialTimeout("unix", serverSocketPath, time.Second)
			if err != nil {
				panic(err)
			}
			<-waiter
			go fakeServer.fakeRabbitMQConnectionOpen(ctx)
			return c, err
		})

		c, err := raw.DialConfig(ctx, conf)
		Expect(err).ToNot(HaveOccurred())
		Expect(c).NotTo(BeNil())

		closedCh := c.NotifyConnectionClosed()

		// abruptly close the connection
		c.(*raw.Client).ForceCloseConnectionSocket()

		// eventually receive notification
		Eventually(closedCh).
			Within(time.Millisecond * 1500).
			Should(Receive())
	}, SpecTimeout(time.Second*3))

	AfterEach(func() {
		Expect(os.RemoveAll(tempLocation)).To(Succeed())
	})
})
