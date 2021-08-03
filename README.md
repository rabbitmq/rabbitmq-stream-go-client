<h1 align="center">RabbitMQ Stream GO Client</h1>

---
<div align="center">

![Build](https://github.com/rabbitmq/rabbitmq-stream-go-client/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/rabbitmq/rabbitmq-stream-go-client/branch/main/graph/badge.svg?token=HZD4S71QIM)](https://codecov.io/gh/rabbitmq/rabbitmq-stream-go-client)

Experimental client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)
</div>

# Table of Contents

- [Overview](#overview)
- [Installing](#installing)
- [Run server with Docker](#run-server-with-docker)
- [Getting started for impatient](#getting-started-for-impatient)
- [Examples](#examples)
- [Usage](#usage)
    * [Connect](#connect)
        * [Multi hosts](#multi-hosts)
        * [Load Balancer](#load-balancer)
        * [TLS](#tls)
    * [Streams](#streams)
    * [Publish messages](#publish-messages)
        * [`Send` vs `BatchSend`] (#send-batchsend)
        * [Confirmation](#confirmation)
        * [Errors](#errors)
        * [Deduplication](#deduplication)
        * [HA producer - Experimental](#haproducer-experimental)
    * [Consume messages](#consume-messages)
        * [Offset](#consume_offset)
        * [Track Offset](#track-offset)
    * [Handle Close](#handle-close)
- [Perf test tool](#perftool)
    * [Perf test tool Docker](#perftooldocker)

# Overview

Experimental client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

# Installing

```shell
go get -u github.com/rabbitmq/rabbitmq-stream-go-client
```

imports:
```golang
"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream" // Main package
"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp" // amqp 1.0 package to encode messages
"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message" // messages interface package, you may not need to import it directly
```

# Run server with Docker
You may need a server to test locally. Let's start the broker:
```shell 
docker run -it --rm --name rabbitmq -p 5552:5552 -p 15672:15672\
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost -rabbit loopback_users "none"' \
    rabbitmq:3.9-rc-management
```
The broker should start in a few seconds. When it’s ready, enable the `stream` plugin and `stream_management`:
```shell
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream_management
```

management UI: http://localhost:15672/
stream port: `rabbitmq-stream://guest:guest@localhost:5552`

# Getting started for impatient

See [getting started](./examples/getting_started.go) example

# Usage

# Connect

Standard way to connect single node:
```golang
env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)
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

# Multi hosts

It is possible to define multi hosts, in case one fails to connect the clients tries random another one.

```golang
addresses := []string{
		"rabbitmq-stream://guest:guest@host1:5552/%2f",
		"rabbitmq-stream://guest:guest@host2:5552/%2f",
		"rabbitmq-stream://guest:guest@host3:5552/%2f"}

		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
```

# Load Balancer

The stream client is supposed to reach all the hostnames,
in case of load balancer you can use the `stream.AddressResolver` parameter in this way:

```golang
addressResolver := stream.AddressResolver{
		Host: "load-balancer-ip",
		Port: 5552,
	}
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("host").
			SetPort(5552).
			SetAddressResolver(addressResolver).
```

In this configuration the client tries the connection until reach the right node.

This [rabbitmq blog post](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/) explains the details.

See also "Using a load balancer" example in the [examples](./examples/) directory

# TLS

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

The `tls.Config` is the standard golang tls library https://pkg.go.dev/crypto/tls
See also "Getting started TLS" example in the [examples](./examples/) directory


# Streams

To define streams you need to use the the `enviroment` interfaces `DeclareStream` and `DeleteStream`.

It is highly recommended to define stream retention policies during the stream creation, like `MaxLengthBytes` or `MaxAge`:

```golang
err = env.DeclareStream(streamName,
		stream.NewStreamOptions().
		SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))
```

Note: The function `DeclareStream` returns `stream.StreamAlreadyExists` if a stream is already defined.


# Publish messages

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


`producer.Send`:
- accepts one message as parameter
- automatically aggregates the messages
- automatically splits  the messages in case the size is bigger than `requestedMaxFrameSize`
- automatically splits the messages based on batch-size
- sends the messages in case nothing happens in `producer-send-timeout`
- is asynchronous

`producer.BatchSend`:
- accepts an array messages as parameter
- is synchronous

# `Send` vs `BatchSend`

The `BatchSend` is the primitive to send the messages, `Send` introduces a smart layer to publish messages and internally uses `BatchSend`.

The `Send` interface works in most of cases, In some condition is about 15/20 slower than `BatchSend`. See also this [thread](https://groups.google.com/g/rabbitmq-users/c/IO_9-BbCzgQ).


### Project status
---
The client is a work in progress, the API(s) could change
