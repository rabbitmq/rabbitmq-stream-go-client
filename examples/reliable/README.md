### Reliable Producer/Consumer example

This example demonstrates how to use reliable producers and consumers to send and receive messages.
The `ReliableProducer` and `ReliableConsumer` are in the `ha` package and use the disconnection event to reconnect to the broker.
You can write your own `ReliableProducer` and `ReliableConsumer` by using the `Close` channel.

The `ReliableProducer` blocks the sending of messages when the broker is disconnected and resumes sending when the broker is reconnected.

In this example, we use `unConfirmedMessages` to re-send the messages that were not confirmed by the broker, for instance, in case of a disconnection.
Then, the `unConfirmedMessages` are sent to the broker again.
Note:
- The `unConfirmedMessages` are not persisted, so if the application is restarted, the `unConfirmedMessages` will be lost.
- The `unConfirmedMessages` order is not guaranteed
- The `unConfirmedMessages` can grow indefinitely if the broker is unavailable for a long time.

The example enables golang `pprof` you can check the url: localhost:6060/debug/pprof/. </br> 
The scope is to check the resources used by the application in case of reconnection.


The `reliable_common.go/retry` function does different checks because during the restart broker can happen different events, please check:
- [this presentation](https://docs.google.com/presentation/d/111PccBLRGb-RNpYEKeIm2MQvdky-fXrQ/edit?usp=sharing&ouid=106772747306273309885&rtpof=true&sd=true) for more details.
- [the code](../../pkg/ha/reliable_common.go) for the implementation details.

### Run the example

The example is meant to be run with a RabbitMQ cluster with HA proxy, please check the [compose/README.md](../../compose/README.md) for more details.
The scope of this example is to show how to use the `ReliableProducer` and `ReliableConsumer` in case of a disconnection, so we will stop the broker and check how the application behaves.

To help a long-running test 
```
make rabbitmq-ha-proxy 
```

Be sure there are no streams or queues created in the RabbitMQ management UI, otherwise the example will not work as expected.

Then run the example:

```bash
go run reliable_client.go 
```

Then close the connections. Note it closes all the connections every `sleep_duration` seconds.
```bash
bash close_all_connections
```

or restart the containers to simulate a broker restart. Note it restarts all the containers in your docker stack even not RabbitMQ instances.
```bash
bash restart_containers
```

I usually avoid to run the `close_all_connections` and `restart_containers` at the same time. 


As result you should see the total confirmation messages equal to the total sent messages, and the total resent messages equal to 0.:
```
MESSAGES
  Target  14,000,000      Sent  14,000,000      ReSent  0

  CONFIRMATIONS
  Confirmed  13,962,568    Failed  37,432        Total  14,000,000
```

Consumer side the number of messages received by each consumer should be equal to the total messages in the management UI:
```
CONSUMPTION
  Total  27,984,820      Per Consumer  13,992,410
```

### Run the example as a Docker container

All the parameters that were hardcoded constants in `reliable_client.go` (connection settings, stream
names, whether to also exercise a super stream, number of partitions, load/concurrency, etc.) can be
overridden with environment variables, so the example can run unattended in a container.

Build the image from the repository root (the build needs the full module, so the Docker build context
must be the repo root, not this directory):

```bash
docker build -f examples/reliable/Dockerfile -t rabbitmq-stream-reliable-client .
```

Run it against a broker, overriding whichever parameters you need:

```bash
docker run --rm \
  -e RABBITMQ_HOST=my-rabbitmq \
  -e RABBITMQ_PORT=5552 \
  -e RABBITMQ_USER=guest \
  -e RABBITMQ_PASSWORD=guest \
  -e STREAM_NAMES=my-stream-1,my-stream-2 \
  -e ADD_SUPER_STREAM=true \
  -e NUMBER_OF_PARTITIONS=3 \
  -e SUPER_STREAM_NAME=my-super-stream \
  -e MESSAGES_TO_SEND=1000000 \
  -p 6060:6060 \
  rabbitmq-stream-reliable-client
```

By default the container runs in silent mode (`IS_SILENT=true`, no stdin prompts) and shuts down
gracefully on `docker stop` (SIGTERM), closing all producers, consumers, and the environment.

Available environment variables:

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Broker host (also used as the `AddressResolver` host, e.g. for a load balancer) |
| `RABBITMQ_PORT` | `5552` | Broker stream port |
| `RABBITMQ_USER` | `guest` | Username |
| `RABBITMQ_PASSWORD` | `guest` | Password |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `RABBITMQ_TLS` | `false` | Enable TLS |
| `RABBITMQ_TLS_SKIP_VERIFY` | `true` | Skip TLS certificate verification when `RABBITMQ_TLS=true` |
| `STREAM_NAMES` | `golang-reliable-Test,golang-reliable-Test-1,golang-reliable-Test-2` | Comma-separated list of stream names to declare and use |
| `ADD_SUPER_STREAM` | `true` | Also exercise a super stream in addition to the streams above |
| `NUMBER_OF_PARTITIONS` | `3` | Number of partitions for the super stream |
| `SUPER_STREAM_NAME` | `golang-reliable-super-stream-Test` | Super stream name |
| `MAX_STREAM_LENGTH_GB` | `10` | Max length (GB) for declared streams/super stream |
| `MESSAGES_TO_SEND` | `1000000` | Number of messages each producer goroutine sends per stream |
| `NUMBER_OF_PRODUCERS` | `2` | Number of reliable producers per stream |
| `CONCURRENT_PRODUCERS` | `1` | Number of concurrent sending goroutines per producer |
| `NUMBER_OF_CONSUMERS` | `2` | Number of reliable consumers per stream |
| `SEND_DELAY_MICROS` | `100` | Delay (microseconds) applied every `DELAY_EACH_MESSAGES` messages |
| `DELAY_EACH_MESSAGES` | `500` | How often (in messages) the send delay is applied |
| `MAX_PRODUCERS_PER_CLIENT` | `2` | Max producers multiplexed per underlying client connection |
| `MAX_CONSUMERS_PER_CLIENT` | `5` | Max consumers multiplexed per underlying client connection |
| `ENABLE_RESEND` | `false` | Resend unconfirmed messages picked up by the confirmation callback |
| `IS_SILENT` | `false` (`true` in the Docker image) | Skip stdin prompts and shut down on SIGINT/SIGTERM instead |
| `STATS_INTERVAL_SECONDS` | `5` | Refresh interval for the dashboard printed to stdout |
| `PPROF_ENABLED` | `true` | Enable the `pprof` HTTP endpoint |
| `PPROF_ADDR` | `localhost:6060` (`0.0.0.0:6060` in the Docker image) | Listen address for the `pprof` HTTP endpoint |

