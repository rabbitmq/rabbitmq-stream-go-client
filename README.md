# GO stream client for RabbitMQ streaming queues
---
![Build](https://github.com/rabbitmq/rabbitmq-stream-go-client/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/Gsantomaggio/go-stream-client/branch/main/graph/badge.svg?token=HZD4S71QIM)](https://codecov.io/gh/Gsantomaggio/go-stream-client)

Experimental client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

### Download
---
```
go get -u github.com/rabbitmq/rabbitmq-stream-go-client@v0.4-alpha
```

### Getting started
---
- Run RabbitMQ docker image with streaming:
   ```
   docker run -it --rm --name rabbitmq -p 5551:5551 -p 5672:5672 -p 15672:15672 \
   -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
   pivotalrabbitmq/rabbitmq-stream
  ```
- Run "getting started" example:
  ```
   go run examples/getting_started.go
  ```

### Performance test tool is an easy way to do some test:
```
go run perfTest/perftest.go silent
```

### API
---

The API are generally composed by mandatory arguments and optional arguments
the optional arguments can be set in the standard go way as:
```golang
env, err := stream.NewEnvironment(
            &stream.EnvironmentOptions{
                    ConnectionParameters:  stream.Broker{
                    Host:     "localhost",
                    Port:     5551,
                    User:     "guest",
                    Password: "guest",
                },
                MaxProducersPerClient: 3,
                MaxConsumersPerClient: 3,
                },
            )
```
or using Builders as:
```golang
env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5551).
			SetUser("guest").
			SetPassword("guest"))
```

`nil` is also a valid value, default values will be provided:
```golang
env, err := stream.NewEnvironment(nil) 
```

The suggested way is to use builders.


### Build from source
---

```shell
make build
```

You need a docker image running to execute the tests in this way:
```
 docker run -it --rm --name rabbitmq -p 5551:5551 \
   -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
   pivotalrabbitmq/rabbitmq-stream
```



 ### Project status
 ---
 The client is a work in progress, the API(s) could change
