package stream_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloseNotificationsAfterBrokerOperations(t *testing.T) {
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, env.Close()) })
	name := fmt.Sprintf("close-notification-%d", time.Now().UnixNano())
	require.NoError(t, env.DeclareStream(name, stream.NewStreamOptions()))
	t.Cleanup(func() { assert.NoError(t, env.DeleteStream(name)) })
	producer, err := env.NewProducer(name, stream.NewProducerOptions())
	require.NoError(t, err)
	require.NoError(t, producer.Close())
	select {
	case event, ok := <-producer.NotifyClose():
		require.True(t, ok)
		assert.Equal(t, stream.DeletePublisher, event.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("producer close before registration was lost")
	}
	consumer, err := env.NewConsumer(name, func(stream.ConsumerContext, *amqp.Message) {}, stream.NewConsumerOptions())
	require.NoError(t, err)
	require.NoError(t, consumer.Close())
	select {
	case event, ok := <-consumer.NotifyClose():
		require.True(t, ok)
		assert.Equal(t, stream.UnSubscribe, event.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer close before registration was lost")
	}
	superName := name + "-super"
	require.NoError(t, env.DeclareSuperStream(superName, stream.NewPartitionsOptions(2)))
	t.Cleanup(func() { assert.NoError(t, env.DeleteSuperStream(superName)) })
	super, err := env.NewSuperStreamConsumer(superName, func(stream.ConsumerContext, *amqp.Message) {}, stream.NewSuperStreamConsumerOptions())
	require.NoError(t, err)
	require.NoError(t, super.Close())
	events := super.NotifyPartitionClose(1)
	var partitions []string
	for range 2 {
		select {
		case event, ok := <-events:
			require.True(t, ok, "partition events were discarded")
			partitions = append(partitions, event.Partition)
		case <-time.After(5 * time.Second):
			t.Fatal("partition close before registration was lost")
		}
	}
	assert.ElementsMatch(t, []string{superName + "-0", superName + "-1"}, partitions)
	select {
	case _, ok := <-events:
		assert.False(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("partition notification channel did not close")
	}
}
