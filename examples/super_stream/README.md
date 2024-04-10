## Super Stream Example

This example demonstrates how to use the [`Super Stream` feature](https://www.rabbitmq.com/blog/2022/07/13/rabbitmq-3-11-feature-preview-super-streams).


### Run the example

1. Start the producer:
```bash
go run producer/producer.go
```

The producer will start sending messages to the stream. It is voluntary slow to make the example easy.

You should see the following output:

```bash
Super stream example - partitions
Connecting to RabbitMQ streaming ...
Message with key: key_0 stored in partition invoices-0, total: 1
Message with key: key_1 stored in partition invoices-1, total: 2
Message with key: key_2 stored in partition invoices-2, total: 3
```

2. Start three consumers in three different terminals:

and you should see the following output:
```bash
[15:19:38] - [ partition: invoices-0] consumer name: MyApplication, data: [hello_super_stream_3], message offset 8398,
 [15:19:39] - [ partition: invoices-0] consumer name: MyApplication, data: [hello_super_stream_4], message offset 8400,
 [15:19:40] - [ partition: invoices-0] consumer name: MyApplication, data: [hello_super_stream_7], message offset 8402,
 [15:19:42] - [ partition: invoices-0] consumer name: MyApplication, data: [hello_super_stream_10], message offset 8404,
 [15:19:42] - [ partition: invoices-0] consumer name: MyApplication, data: [hello_super_stream_11], message offset 8406,
 ```

```bash
[15:19:29] - Consumer update for: invoices-1. The cosumer is now active ....Restarting from offset: offset, value: 8628
[15:19:37] - [ partition: invoices-1] consumer name: MyApplication, data: [hello_super_stream_1], message offset 8638,
[15:19:41] - [ partition: invoices-1] consumer name: MyApplication, data: [hello_super_stream_8], message offset 8640,
[15:19:41] - [ partition: invoices-1] consumer name: MyApplication, data: [hello_super_stream_9], message offset 8642,
[15:19:43] - [ partition: invoices-1] consumer name: MyApplication, data: [hello_super_stream_12], message offset 8644,
```


```bash
[15:19:38] - [ partition: invoices-2] consumer name: MyApplication, data: [hello_super_stream_2], message offset 8501,
 [15:19:39] - [ partition: invoices-2] consumer name: MyApplication, data: [hello_super_stream_5], message offset 8503,
 [15:19:40] - [ partition: invoices-2] consumer name: MyApplication, data: [hello_super_stream_6], message offset 8505,
 [15:19:43] - [ partition: invoices-2] consumer name: MyApplication, data: [hello_super_stream_13], message offset 8507,
 [15:19:44] - [ partition: invoices-2] consumer name: MyApplication, data: [hello_super_stream_15], message offset 8509,
```


Stop random consumers and see how the system rebalances the partitions.
