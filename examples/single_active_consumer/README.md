## Single Active Consumer Example

This example demonstrates how to use the `Single Active Consumer` pattern to ensure that only one consumer processes messages from a stream at a time.

### Run the example


1. Start the producer:
```bash
go run producer.go
```
The producer will start sending messages to the stream. It is voluntary slow to make the example easy.
You should see the following output:

```bash
go run producer.go
Producer for Single Active Consumer example
Connecting to RabbitMQ streaming ...
[08:16:20] sending message hello_world_0 ...
[08:16:20] message [hello_world_0] stored
[08:16:23] sending message hello_world_1 ...
[08:16:23] message [hello_world_1] stored
[08:16:26] sending message hello_world_2 ...
[08:16:26] message [hello_world_2] stored
[08:16:29] sending message hello_world_3 ...
[08:16:29] message [hello_world_3] stored
[08:16:32] sending message hello_world_4 ...
[08:16:32] message [hello_world_4] stored
[08:16:35] sending message hello_world_5 ...
[08:16:35] message [hello_world_5] stored
[08:16:38] sending message hello_world_6 ...
[08:16:38] message [hello_world_6] stored
```
2. Start the consumer:
In a new terminal, run the consumer:
```bash
go run single_active_consumer.go myFirstConsumer
```

You should see the following output:

```bash
Single Active Consumer example.
Connecting to RabbitMQ streaming ...
Press any key to stop
Single Active Consumer example.
Connecting to RabbitMQ streaming ...
Press any key to stop
[08:16:32] - Consumer promoted. Active status: true
[08:16:32] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_0], message offset 0,
 [08:16:32] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_1], message offset 1,
 [08:16:32] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_2], message offset 2,
 [08:16:32] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_3], message offset 3,
 [08:16:32] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_4], message offset 4,
 [08:16:35] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_5], message offset 6,
 [08:16:38] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_6], message offset 8,
 [08:16:41] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_7], message offset 10,
 [08:16:44] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_8], message offset 12,
 [08:16:47] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_9], message offset 14,
 [08:16:50] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_10], message offset 16,
 [08:16:53] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_11], message offset 18,
 [08:16:56] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_12], message offset 20,
 [08:16:59] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_13], message offset 22,
 [08:17:02] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_14], message offset 24,
 [08:17:05] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_15], message offset 26,
 [08:17:08] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_16], message offset 28,
 [08:17:11] - [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_17], message offset 30,
```

3. Start a second consumer:
In a new terminal, run the consumer:
```bash
go run single_active_consumer.go mySecondConsumer
```

You should see the following output:

```bash
Single Active Consumer example.
Connecting to RabbitMQ streaming ...
Press any key to stop
```

4. Stop the first consumer:

In the first terminal, press any key to stop the first consumer. <br />
The second consumer should be promoted to active status and restart processing messages from the last stored offset.

You should see:
```bash
[08:17:11] - Consumer promoted. Active status: true
 [08:17:14] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_18], message offset 32,
 [08:17:17] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_19], message offset 34,
 [08:17:20] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_20], message offset 36,
 [08:17:23] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_21], message offset 38,
 [08:17:26] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_22], message offset 40,
 [08:17:29] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_23], message offset 42,
 [08:17:32] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_24], message offset 44,
 [08:17:35] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_25], message offset 46,
 [08:17:38] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_26], message offset 48,
 [08:17:41] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_27], message offset 50,
 [08:17:44] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_28], message offset 52,
 [08:17:47] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_29], message offset 54,
 [08:17:50] - [ mySecondConsumer ] - consumer name: MyApplication, data: [hello_world_30], message offset 56,
```

