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


The `reliable_common.go/retry` function does different checks because during the restart broker can happen different events, please check:
- [this presentation](https://docs.google.com/presentation/d/111PccBLRGb-RNpYEKeIm2MQvdky-fXrQ/edit?usp=sharing&ouid=106772747306273309885&rtpof=true&sd=true) for more details.
- [the code](../../pkg/ha/reliable_common.go) for the implementation details.

