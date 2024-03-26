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
Producer for Single Active Consumer example
Connecting to RabbitMQ streaming ...
sending message hello_world_0 ...
message [hello_world_0] stored
sending message hello_world_1 ...
message [hello_world_1] stored
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
Consumer promoted. Active status: true
 [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_0], message offset 0,
 [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_1], message offset 1,
 [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_2], message offset 2,
 [ myFirstConsumer ] - consumer name: MyApplication, data: [hello_world_3], message offset 3,
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

In the first terminal, press any key to stop the first consumer. 
The second consumer should be promoted to active status and start processing messages.

