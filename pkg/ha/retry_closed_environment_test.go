package ha

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// closedEnvReliable counts the instances retry would recreate on its environment.
type closedEnvReliable struct {
	env       *stream.Environment
	status    atomic.Int32
	instances atomic.Int32
}

func (r *closedEnvReliable) setStatus(value int)         { r.status.Store(int32(value)) }
func (r *closedEnvReliable) getInfo() string             { return "closed-environment-reliable" }
func (r *closedEnvReliable) getEnv() *stream.Environment { return r.env }
func (r *closedEnvReliable) getTimeOut() time.Duration   { return time.Second }
func (r *closedEnvReliable) GetStatus() int              { return int(r.status.Load()) }
func (r *closedEnvReliable) GetStreamName() string       { return "" }
func (r *closedEnvReliable) GetStatusAsString() string   { return getStatusAsString(r) }
func (r *closedEnvReliable) getNewInstance(string) newEntityInstance {
	return func() error {
		r.instances.Add(1)
		return nil
	}
}

var _ = Describe("Reliable retry", func() {
	It("stops on a closed environment instead of reconnecting", func() {
		env, err := stream.NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).To(Succeed())
		DeferCleanup(func() {
			cleanup, err := stream.NewEnvironment(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanup.DeleteStream(streamName)).To(Succeed())
			Expect(cleanup.Close()).To(Succeed())
		})
		Expect(env.Close()).To(Succeed())

		type result struct {
			err         error
			reconnected bool
		}
		r := &closedEnvReliable{env: env}
		finished := make(chan result, 1)
		go func() {
			err, reconnected := retry(1, r, streamName)
			finished <- result{err: err, reconnected: reconnected}
		}()

		// retry always waits its first backoff (3-11 seconds) before the lookup
		var res result
		Eventually(finished, 30*time.Second).Should(Receive(&res))
		Expect(res.err).To(MatchError(stream.AlreadyClosed))
		Expect(res.reconnected).To(BeFalse())
		Expect(r.instances.Load()).To(BeZero(), "retry recreated an instance on a closed environment")
	})
})
