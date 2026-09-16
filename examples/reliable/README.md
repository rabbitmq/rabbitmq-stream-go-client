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

