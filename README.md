<h1 align="center">RabbitMQ Stream GO Client</h1>

---
<div align="center">

![Build](https://github.com/rabbitmq/rabbitmq-stream-go-client/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/rabbitmq/rabbitmq-stream-go-client/branch/main/graph/badge.svg?token=HZD4S71QIM)](https://codecov.io/gh/rabbitmq/rabbitmq-stream-go-client)

Experimental client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)
</div>

### Install
---

```
go get -u github.com/rabbitmq/rabbitmq-stream-go-client@v0.10-alpha
```

### Getting started
---
See [Getting Started](./examples/getting_started.go) example

### Examples
---
See [examples](./examples/) for more use cases

### Docker Image:
---
Exercising a stream is very easy with Docker.
Let's start the broker:
```bash 
docker run -it --rm --network rabbitmq-streams --name rabbitmq \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbit loopback_users "none"' \
    rabbitmq:3.9-rc
```
The broker should start in a few seconds. When it’s ready, enable the stream plugin:
```bash
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
```

### Documentation

Full documentation on wiki page

### Project status
---
The client is a work in progress, the API(s) could change
