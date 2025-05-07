<h1 align="center">RabbitMQ Stream GO Client</h1>

---
<div align="center">

![Build](https://github.com/rabbitmq/rabbitmq-stream-go-client/actions/workflows/build_and_test.yml/badge.svg)

Go client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)
</div>

# Table of Contents

- [Overview](#overview)
- [Installing](#installing)
- [Run server with Docker](#run-server-with-docker)
- [Getting started for impatient](#getting-started-for-impatient)
- [Examples](#examples)
- [Client best practices](#client-best-practices)
- [Usage](#usage)
    * [Connect](#connect)
        * [Multi hosts](#multi-hosts)
        * [Load Balancer](#load-balancer)
        * [TLS](#tls)
        * [Sasl Mechanisms](#sasl-mechanisms)
        * [Streams](#streams)
            * [Statistics](#streams-statistics)
    * [Publish messages](#publish-messages)
        * [`Send` vs `BatchSend`](#send-vs-batchsend)
        * [Publish Confirmation](#publish-confirmation)
        * [Deduplication](#deduplication)
        * [Sub Entries Batching](#sub-entries-batching)
        * [Publish Filtering](#publish-filtering)
    * [Consume messages](#consume-messages)
        * [Manual Track Offset](#manual-track-offset)
        * [Automatic Track Offset](#automatic-track-offset)
        * [Get consumer Offset](#get-consumer-offset)
        * [Consume Filtering](#consume-filtering)
        * [Single Active Consumer](#single-active-consumer)
    * [Handle Close](#handle-close)
    * [Reliable Producer and Reliable Consumer](#reliable-producer-and-reliable-consumer)
    * [Super Stream](#super-stream)
- [Performance test tool](#performance-test-tool)
    * [Performance test tool Docker](#performance-test-tool-docker)
- [Build form source](#build-form-source)
- [Project status](#project-status)

### Overview

Go client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)
The client contains all features to interact with the RabbitMQ Stream Queues. </br>

The main structure is the `Environment` that contains the `Producer` and `Consumer` interfaces. </br>

`Producer` and `Consumer` are the main interfaces to interact with the RabbitMQ Stream Queues. </br>
They don't support the auto-reconnect in case of disconnection but have the events to detect it.</br>

The client provides the `ReliableProducer` and `ReliableConsumer` that support the auto-reconnect in case of
disconnection.</br>
See also the [Reliable Producer and Reliable Consumer](#reliable-producer-and-reliable-consumer) section.

### Installing

```shell
 go get -u github.com/rabbitmq/rabbitmq-stream-go-client
```

imports:

```golang
"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"  // Main package
"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"    // amqp 1.0 package to encode messages
"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message" // messages interface package, you may not need to import it directly
```

### Run server with Docker
---
You may need a server to test locally. Let's start the broker:

```shell
docker run -it --rm --name rabbitmq -p 5552:5552 -p 15672:15672\
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost -rabbit loopback_users "none"' \
    rabbitmq:4-management
```

The broker should start in a few seconds. When it’s ready, enable the `stream` plugin and `stream_management`:

```shell
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream_management
```

Management UI: http://localhost:15672/ </br>
Stream uri: `rabbitmq-stream://guest:guest@localhost:5552`

### Getting started for impatient

- [Getting started with reliable producer/consumer](./examples/reliable_getting_started/reliable_getting_started.go)
  example.
- [Getting started with standard producer/consumer](./examples/getting_started/getting_started.go) example.
- Getting started Video tutorial:

[![Getting Started](https://img.youtube.com/vi/8qfvl6FgC50/0.jpg)](https://www.youtube.com/watch?v=8qfvl6FgC50)

### Examples

See [examples](./examples/) directory for more use cases.

### Client best practices

This client provides a set of best practices to use the client in the best way. </br>
See [best practices](./best_practices/README.md) for more details.

# Usage

### Connect

Standard way to connect single node:

```golang
env, err := stream.NewEnvironment(
    stream.NewEnvironmentOptions().
    SetHost("localhost").
    SetPort(5552).
    SetUser("guest").
    SetPassword("guest"))
```

you can define the number of producers per connections, the default value is 1:

```golang
stream.NewEnvironmentOptions().
SetMaxProducersPerClient(2))
```

you can define the number of consumers per connections, the default value is 1:

```golang
stream.NewEnvironmentOptions().
SetMaxConsumersPerClient(2))
```

To have the best performance you should use the default values.
Note about multiple consumers per connection:
*The IO threads is shared across the consumers, so if one consumer is slow it could impact other consumers performances*

### Multi hosts

It is possible to define multi hosts, in case one fails to connect the clients tries random another one.

```golang
addresses := []string{
"rabbitmq-stream://guest:guest@host1:5552/%2f",
"rabbitmq-stream://guest:guest@host2:5552/%2f",
"rabbitmq-stream://guest:guest@host3:5552/%2f"}

env, err := stream.NewEnvironment(
stream.NewEnvironmentOptions().SetUris(addresses))
```

### Load Balancer

The stream client is supposed to reach all the hostnames,
in case of load balancer you can use the `stream.AddressResolver` parameter in this way:

```golang
addressResolver := stream.AddressResolver{
Host: "load-balancer-ip",
Port: 5552,
}
env, err := stream.NewEnvironment(
    stream.NewEnvironmentOptions().
    SetHost(addressResolver.Host).
    SetPort(addressResolver.Port).
    SetAddressResolver(addressResolver).
```

In this configuration the client tries the connection until reach the right node.

This [rabbitmq blog post](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/) explains the details.

See also "Using a load balancer" example in the [examples](./examples/) directory

### TLS

To configure TLS you need to set the `IsTLS` parameter:

```golang
env, err := stream.NewEnvironment(
stream.NewEnvironmentOptions().
    SetHost("localhost").
    SetPort(5551). // standard TLS port
    SetUser("guest").
    SetPassword("guest").
    IsTLS(true).
    SetTLSConfig(&tls.Config{}),
)
```

The `tls.Config` is the standard golang tls library https://pkg.go.dev/crypto/tls </br>
See also "Getting started TLS" example in the [examples](./examples/) directory. </br>

It is also possible to configure TLS using the Schema URI like:

```golang
env, err := stream.NewEnvironment(
stream.NewEnvironmentOptions().
SetUri("rabbitmq-stream+tls://guest:guest@localhost:5551/").
SetTLSConfig(&tls.Config{}),
)
```

### Sasl Mechanisms

To configure SASL you need to set the `SaslMechanism` parameter `Environment.SetSaslConfiguration`:

```golang
cfg := new(tls.Config)
cfg.ServerName = "my_server_name"
cfg.RootCAs = x509.NewCertPool()

if ca, err := os.ReadFile("certs/ca_certificate.pem"); err == nil {
    cfg.RootCAs.AppendCertsFromPEM(ca)
}

if cert, err := tls.LoadX509KeyPair("certs/client/cert.pem", "certs/client/key.pem"); err == nil {
    cfg.Certificates = append(cfg.Certificates, cert)
}

env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
    SetUri("rabbitmq-stream+tls://my_server_name:5551/").
    IsTLS(true).
    SetSaslConfiguration(stream.SaslConfigurationExternal). // SASL EXTERNAL
    SetTLSConfig(cfg))
```

### Streams

To define streams you need to use the the `environment` interfaces `DeclareStream` and `DeleteStream`.

It is highly recommended to define stream retention policies during the stream creation, like `MaxLengthBytes` or
`MaxAge`:

```golang
err = env.DeclareStream(streamName,
stream.NewStreamOptions().
SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))
```

The function `DeclareStream` doesn't return errors if a stream is already defined with the same parameters.
Note that it returns the precondition failed when it doesn't have the same parameters
Use `StreamExists` to check if a stream exists.

### Streams Statistics

To get stream statistics you need to use the `environment.StreamStats` method.

```golang
stats, err := environment.StreamStats(testStreamName)

// FirstOffset - The first offset in the stream.
// return first offset in the stream /
// Error if there is no first offset yet

firstOffset, err := stats.FirstOffset() // first offset of the stream

// CommittedChunkId - The ID (offset) of the committed chunk (block of messages) in the stream.
//
//	It is the offset of the first message in the last chunk confirmed by a quorum of the stream
//	cluster members (leader and replicas).
//
//	The committed chunk ID is a good indication of what the last offset of a stream can be at a
//	given time. The value can be stale as soon as the application reads it though, as the committed
//	chunk ID for a stream that is published to changes all the time.

committedChunkId, err := statsAfter.CommittedChunkId()
```

### Publish messages

To publish a message you need a `*stream.Producer` instance:

```golang
producer, err := env.NewProducer("my-stream", nil)
```

With `ProducerOptions` is possible to customize the Producer behaviour.

The client provides two interfaces to send messages.
`send`:

```golang
var message message.StreamMessage
message = amqp.NewMessage([]byte("hello"))
err = producer.Send(message)
```

and `BatchSend`:

```golang
var messages []message.StreamMessage
for z := 0; z < 10; z++ {
    messages = append(messages, amqp.NewMessage([]byte("hello")))
}
err = producer.BatchSend(messages)
```

### `Send` vs `BatchSend`

The `BatchSend` is the primitive to send the messages. It is up to the user to manage the aggregation.
`Send` introduces a smart layer to publish messages and internally uses `BatchSend`.

Starting from version 1.5.0, the `Send` uses a dynamic send.
The client sends the message buffer regardless of any timeout.</br>

What should you use? </br>
The `Send` method is the best choice for most of the cases:</br>

- It is asynchronous
- It is smart to aggregate the messages in a batch with a low-latency
- It is smart to split the messages in case the size is bigger than `requestedMaxFrameSize`
- You can play with `BatchSize` parameter to increase the throughput

The `BatchSend` is useful in case you need to manage the aggregation by yourself. </br>
It gives you more control over the aggregation process: </br>

- It is synchronous
- It is up to the user to manage the aggregation
- It is up to the user to split the messages in case the size is bigger than `requestedMaxFrameSize`
- It can be faster than `Send` in case the aggregation is managed by the user.

#### Throughput vs Latency</br>

With both methods you can have low-latency and/or high-throughput. </br>
The `Send` is the best choice for low-latency without care about aggregation.
With `BatchSend` you have more control.</br>

Performance test tool can help you to test `Send` and `BatchSend` </br>
See also the [Performance test tool](#performance-test-tool) section.

### Publish Confirmation

For each publish the server sends back to the client the confirmation or an error.
The client provides an interface to receive the confirmation:

```golang
//optional publish confirmation channel
chPublishConfirm := producer.NotifyPublishConfirmation()
handlePublishConfirm(chPublishConfirm)

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
go func () {
    for confirmed := range confirms {
        for _, msg := range confirmed {
            if msg.IsConfirmed() {
                fmt.Printf("message %s stored \n  ", msg.GetMessage().GetData())
            } else {
                fmt.Printf("message %s failed \n  ", msg.GetMessage().GetData())
        }
    }
    }
}()
}
```

In the MessageStatus struct you can find two `publishingId`:

```golang
//first one
messageStatus.GetMessage().GetPublishingId()
// second one
messageStatus.GetPublishingId()
```

The first one is provided by the user for special cases like Deduplication.
The second one is assigned automatically by the client.
In case the user specifies the `publishingId` with:

```golang
msg = amqp.NewMessage([]byte("mymessage"))
msg.SetPublishingId(18) // <---
```

The filed: `messageStatus.GetMessage().HasPublishingId()` is true and </br>
the values `messageStatus.GetMessage().GetPublishingId()` and `messageStatus.GetPublishingId()` are the same.

See also "Getting started" example in the [examples](./examples/) directory

### Deduplication

The deduplication is a feature that allows to avoid the duplication of messages. </br>
It is enabled by the user by setting the producer name with the options: </br>

```golang
producer, err := env.NewProducer(streamName, stream.NewProducerOptions().SetName("my_producer"))
```

The stream plugin can handle deduplication data, see this blog post for more details:
https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-message-deduplication/ </br>
You can find a "Deduplication" example in the [examples](./examples/) directory. </br>
Run it more than time, the messages count will be always 10.

To retrieve the last sequence id for producer you can use:

```golang
publishingId, err := producer.GetLastPublishingId()
```

### Sub Entries Batching

The number of messages to put in a sub-entry. A sub-entry is one "slot" in a publishing frame,
meaning outbound messages are not only batched in publishing frames, but in sub-entries as well.
Use this feature to increase throughput at the cost of increased latency. </br>
You can find a "Sub Entries Batching" example in the [examples](./examples/) directory. </br>

Default compression is `None` (no compression) but you can define different kind of compressions: `GZIP`,`SNAPPY`,`LZ4`,
`ZSTD` </br>
Compression is valid only is `SubEntrySize > 1`

```golang
producer, err := env.NewProducer(streamName, stream.NewProducerOptions().
        SetSubEntrySize(100).
        SetCompression(stream.Compression{}.Gzip()))
```

### Publish Filtering

Stream filtering is a new feature in RabbitMQ 3.13. It allows to save bandwidth between the broker and consuming
applications when those applications need only a subset of the messages of a stream.
See this [blog post](https://www.rabbitmq.com/blog/2023/10/16/stream-filtering) for more details.
The blog post also contains a Java example but the Go client is similar.
See the [Filtering](./examples/filtering/filtering.go) example in the [examples](./examples/) directory.

### Consume messages

In order to consume messages from a stream you need to use the `NewConsumer` interface, ex:

```golang
handleMessages := func (consumerContext stream.ConsumerContext, message *amqp.Message) {
    fmt.Printf("consumer name: %s, text: %s \n ", consumerContext.Consumer.GetName(), message.Data)
    }

consumer, err := env.NewConsumer(
                "my-stream",
                handleMessages,
....
```

With `ConsumerOptions` it is possible to customize the consumer behaviour.

```golang
  stream.NewConsumerOptions().
SetConsumerName("my_consumer").                  // set a consumer name
SetCRCCheck(false).                              // Enable/Disable the CRC control.
SetOffset(stream.OffsetSpecification{}.First())) // start consuming from the beginning
```

Disabling the CRC control can increase the performances.

See also "Offset Start" example in the [examples](./examples/) directory

Close the consumer:
`consumer.Close()` the consumer is removed from the server. TCP connection is closed if there aren't </b>
other consumers

### Manual Track Offset

The server can store the current delivered offset given a consumer, in this way:

```golang
handleMessages := func (consumerContext stream.ConsumerContext, message *amqp.Message) {
if atomic.AddInt32(&count, 1)%1000 == 0 {
err := consumerContext.Consumer.StoreOffset() // commit all messages up to the current message's offset
....

consumer, err := env.NewConsumer(
..
stream.NewConsumerOptions().
SetConsumerName("my_consumer").<------
```

A consumer must have a name to be able to store offsets. <br>
Note: *AVOID to store the offset for each single message, it will reduce the performances*

See also "Offset Tracking" example in the [examples](./examples/) directory

The server can also store a previous delivered offset rather than the current delivered offset, in this way:

```golang
processMessageAsync := func (consumer stream.Consumer, message *amqp.Message, offset int64) {
....
err := consumer.StoreCustomOffset(offset) // commit all messages up to this offset
....
```

This is useful in situations where we have to process messages asynchronously and we cannot block the original message
handler. Which means we cannot store the current or latest delivered offset as we saw in the `handleMessages` function
above.

It is possible to store the offset using the Environment interface:
This method is useful in case you want to store the offset for a consumer that is not active anymore.</br>
Note: *AVOID to store the offset for each single message, it will reduce the performances. This method opens and close a
connection each time*. </br>

```golang
    env.StoreOffset(consumerName, streamName, 123)
```

The `StoreOffset` does not return any application error. For example: If the stream does not exist the API doesn't
return an error </br>

### Automatic Track Offset

The following snippet shows how to enable automatic tracking with the defaults:

```golang
stream.NewConsumerOptions().
SetConsumerName("my_consumer").
SetAutoCommit(stream.NewAutoCommitStrategy() ...
```

`nil` is also a valid value. Default values will be used

```golang
stream.NewConsumerOptions().
SetConsumerName("my_consumer").
SetAutoCommit(nil) ...
```

Set the consumer name (mandatory for offset tracking) </br>

The automatic tracking strategy has the following available settings:

- message count before storage: the client will store the offset after the specified number of messages, </br>
  right after the execution of the message handler. The default is every 10,000 messages.

- flush interval: the client will make sure to store the last received offset at the specified interval. </br>
  This avoids having pending, not stored offsets in case of inactivity. The default is 5 seconds.

Those settings are configurable, as shown in the following snippet:

```golang
stream.NewConsumerOptions().
// set a consumerOffsetNumber name
SetConsumerName("my_consumer").
SetAutoCommit(stream.NewAutoCommitStrategy().
SetCountBeforeStorage(50). // store each 50 messages stores
SetFlushInterval(10*time.Second)). // store each 10 seconds
SetOffset(stream.OffsetSpecification{}.First()))
```

See also "Automatic Offset Tracking" example in the [examples](./examples/) directory

### Get consumer offset

It is possible to query the consumer offset using:

```golang
offset, err := env.QueryOffset("consumer_name", "streamName")
```

An error is returned if the offset doesn't exist.

### Consume Filtering

Stream filtering is a new feature in RabbitMQ 3.13. It allows to save bandwidth between the broker and consuming
applications when those applications need only a subset of the messages of a stream.
See this [blog post](https://www.rabbitmq.com/blog/2023/10/16/stream-filtering) for more details.
The blog post also contains a Java example but the Go client is similar.
See the [Filtering](./examples/filtering/filtering.go) example in the [examples](./examples/) directory.

### Single Active Consumer

The Single Active Consumer pattern ensures that only one consumer processes messages from a stream at a time.
See the [Single Active Consumer](./examples/single_active_consumer) example.

To create a consumer with the Single Active Consumer pattern, you need to set the `SingleActiveConsumer` option:

```golang
    consumerName := "MyFirstGroupConsumer"
consumerUpdate := func (isActive bool) stream.OffsetSpecification {..}
stream.NewConsumerOptions().
SetConsumerName(consumerName).
SetSingleActiveConsumer(
stream.NewSingleActiveConsumer(consumerUpdate))
```

The `ConsumerUpdate` function is called when the consumer is promoted. </br>
The new consumer will restart consuming from the offset returned by the `consumerUpdate` function. </br>
It is up to the user to decide the offset to return. </br>
One of the way is to store the offset server side and restart from the last offset. </br>
The [Single Active Consumer](./examples/single_active_consumer) example uses the server side
offset to restart the consumer.

The `ConsumerName` is mandatory to enable the SAC. It is the way to create different group of consumers</br>
Different groups of consumers can consume the same stream at the same time. </br>

The `NewConsumerOptions().SetOffset()` is not necessary when the SAC is active the `ConsumerUpdate` function
replaces the value.

See also this post for more
details: https://www.rabbitmq.com/blog/2022/07/05/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams

### Handle Close

Client provides an interface to handle the producer/consumer close.

```golang
channelClose := consumer.NotifyClose()
defer consumerClose(channelClose)
func consumerClose(channelClose stream.ChannelClose) {
event := <-channelClose
fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
}
```

In this way it is possible to handle fail-over

### Reliable Producer and Reliable Consumer

The `ReliableProducer` and `ReliableConsumer` are built up the standard producer/consumer. </br>
Both use the standard events to handle the close. So you can write your own code to handle the fail-over. </br>

Features:

- [`Both`] auto-reconnect in case of disconnection.
- [`Both`] check if stream exists, if not they close the `ReliableProducer` and `ReliableConsumer`.
- [`Both`] check if the stream has a valid leader and replicas, if not they retry until the stream is ready.
- [`ReliableProducer`] handle the unconfirmed messages automatically in case of fail.
- [`ReliableConsumer`] restart from the last offset in case of restart.

You can find a "Reliable" example in the [examples](./examples/) directory. </br>

### Super Stream

The Super Stream feature is a new feature in RabbitMQ 3.11. It allows to create a stream with multiple partitions. </br>
Each partition is a separate stream, but the client sees the Super Stream as a single stream. </br>

You can find a "Super Stream" example in the [examples](./examples/super_stream) directory. </br>

In this blog post you can find more
details: https://www.rabbitmq.com/blog/2022/07/13/rabbitmq-3-11-feature-preview-super-streams

You can read also the java stream-client blog
post: https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#super-streams

- The code is written in Java but the same concepts are valid for the Go client.
- The Go client has the same features as the Java client.

Super Stream supports [publish-filtering](#publish-filtering) and [consume-filtering](#consume-filtering) features.

Offset tracking is supported for the Super Stream consumer. </br>
In the same way as the standard stream, you can use the `SetAutoCommit` or `SetManualCommit` option to enable/disable
the automatic offset tracking. </br>

On the super stream consumer message handler is possible to identify the partition, the consumer and the offset: </br>

```golang
    handleMessages := func (consumerContext stream.ConsumerContext, message *amqp.Message) {
....
consumerContext.Consumer.GetName() // consumer name 
consumerContext.Consumer.GetOffset() // current offset
consumerContext.Consumer.GetStreamName() // stream name  (partition name )
....
}
```

Manual tracking API:

- `consumerContext.Consumer.StoreOffset()`: stores the current offset.
- `consumerContext.Consumer.StoreCustomOffset(xxx)` stores a custom offset.

Like the standard stream, you should avoid to store the offset for each single message: it will reduce the performances.

### Performance test tool

Performance test tool it is useful to execute tests.
The performance test tool is in the [perfTest](./perfTest) directory. </br>
See also
the [Java Performance](https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#the-performance-tool)
tool

### Build form source

```shell
make build
```

To execute the tests you need a docker image, you can use:

```shell
make rabbitmq-server
```

to run a ready rabbitmq-server with stream enabled for tests.

then `make test`
