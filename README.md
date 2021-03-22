# GO stream client for RabbitMQ streaming queues
---
![Build](https://github.com/Gsantomaggio/go-stream-client/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/Gsantomaggio/go-stream-client/branch/main/graph/badge.svg?token=HZD4S71QIM)](https://codecov.io/gh/Gsantomaggio/go-stream-client)

A POC client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

### How to easy test
---
- Run RabbitMQ docker image with streaming:
   ```
   docker run -it --rm --name rabbitmq -p 5551:5551 -p 5672:5672 -p 15672:15672 \
   -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
   pivotalrabbitmq/rabbitmq-stream
 
  ```
- Run getting started example:
  ```
   go run examples/getting_started.go
  ```

### Methods Implemented:
---
 - Open(vhost)
 - CreateStream (basic version)
 - DeleteStream
 - DeclarePublisher
 - Close Publisher
 - Publish
 - Subscribe (basic version)
 - UnSubscribe
 - HeartBeat
 
 ### Work in Progress
 ---
 The POC is a work in progress, the code isn't too accurate
