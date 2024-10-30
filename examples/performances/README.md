## Client performances

This document describes how to tune the parameters of the client to:
- Increase the throughput 
- And/or reduce the latency 
- And/or reduce the disk space used by the messages.

### Throughput and Latency

The parameters that can be tuned are:
- `SetBatchSize(batchSize)` and `SetBatchPublishingDelay(100)` when use the `Send()` method
- The number of the messages when use the `BatchSend()` method

In this example you can play with the parameters to see the impact on the performances.
There is not a magic formula to find the best parameters, you need to test and find the best values for your use case.

### How to run the example
```
go run performances.go  async 1000000 100;
```

### Disk space used by the messages

The client supports also the batch entry size and the compression:
`SetSubEntrySize(500).SetCompression(stream.Compression{}...`
These parameters can be used to reduce the space used by the messages due of the compression and the batch entry size.


### Default values

The default producer values are meant to be a good trade-off between throughput and latency.
You can tune the parameters to increase the throughput, reduce the latency or reduce the disk space used by the messages.



### Load tests
To execute load tests, you can use the official load test tool:
https://github.com/rabbitmq/rabbitmq-stream-perf-test