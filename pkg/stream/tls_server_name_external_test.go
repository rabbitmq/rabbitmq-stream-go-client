package stream_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("TLS server name", func() {
	DescribeTable("verifies the broker certificate against the connection host",
		func(serverName string, handshakeMatcher types.GomegaMatcher) {
			certificate := newLocalhostCertificate()
			roots := x509.NewCertPool()
			roots.AddCert(certificate.Leaf)

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(listener.Close)

			// the fake broker only speaks TLS: it reports the handshake
			// result and discards whatever the client writes afterwards
			handshakes := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				conn, err := listener.Accept()
				if err != nil {
					handshakes <- err
					return
				}
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
				handshakes <- tlsConn.Handshake()
				_, _ = io.Copy(io.Discard, tlsConn)
			}()

			config := &tls.Config{RootCAs: roots, ServerName: serverName}
			port := listener.Addr().(*net.TCPAddr).Port
			_, err = stream.NewEnvironment(stream.NewEnvironmentOptions().
				SetUri(fmt.Sprintf("rabbitmq-stream+tls://guest:guest@localhost:%d/", port)).
				SetTLSConfig(config).
				SetRPCTimeout(200 * time.Millisecond))
			// the fake broker never answers the stream protocol
			Expect(err).To(HaveOccurred())

			var handshakeErr error
			Eventually(handshakes, 5*time.Second).Should(Receive(&handshakeErr))
			Expect(handshakeErr).To(handshakeMatcher)
			Expect(config.ServerName).To(Equal(serverName), "the caller TLS config was mutated")
		},
		Entry("infers the server name when it is not set", "", Succeed()),
		Entry("honours an explicit server name", "wrong.invalid",
			MatchError(ContainSubstring("bad certificate"))),
	)
})

func newLocalhostCertificate() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())
	leaf, err := x509.ParseCertificate(der)
	Expect(err).NotTo(HaveOccurred())

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}
