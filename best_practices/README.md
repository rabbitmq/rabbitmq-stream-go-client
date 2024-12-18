Client best practices
=====================

The scope of this document is to provide a set of best practices for the client applications that use the Go client library.</br>


#### General recommendations
- Messages are not thread-safe, you should not share the same message between different go-routines or different Send/BatchSend calls.
- Use the producer name only if you need deduplication.
- Avoid to store the consumer offset to the server too often.
- `Send` works well in most of the cases, use `BatchSend` when you need more control.
- Connections/producers/consumers are designed to be long-lived. You should avoid creating and closing them too often.
- The library is generally thread-safe,even it is better to use one producer/consumer per go-routine.

#### Default configuration

The default configuration of the client library is designed to be used in most of the cases.
No particular tuning is required. Just follow the [Getting started](../examples/getting_started.go) example.

#### Multiple producers and consumers

Each connection can support multiple producers and consumers, you can reduce the number of connections by using the same connection for multiple producers and consumers.</br>
With:
```golang
    SetMaxConsumersPerClient(10).
	SetMaxConsumersPerClient(10)
```
The TCP connection will be shared between the producers and consumers.
Note about consumers: One slow consumer can block the others, so it is important:
- To have a good balance between the number of consumers and the speed of the consumers.
- work application side to avoid slow consumers, for example, by using a go-routines/buffers.

#### High throughput

To achieve high throughput, you should use one producer per connection, and one consumer per connection.
This will avoid lock contention between the producers when sending messages and between the consumers when receiving messages.

The method `Send` is usually enough to achieve high throughput. 
In some case you can use the `BatchSend` method. See the `Send` vs `BatchSend` documentation for more details.

#### Low latency

To achieve Low latency, you should use one producer per connection, and one consumer per connection.

The method `Send` is the best choice to achieve low latency. Default values are tuned for low latency.
You can change the `BatchSize` parameter to increase or reduce the max number of messages sent in one batch.
Note: Since the client uses dynamic send, the `BatchSize` parameter is a hint to the client, the client can send less than the `BatchSize`.

#### Store several text based messages 

In case you want to store logs, text-based or big messages, you can use the `Sub Entries Batching` method.
Where it is possible to store multiple messages in one entry and compress the entry with different algorithms.
It is useful to reduce the  disk space and the network bandwidth.
See the `Sub Entries Batching` documentation for more details.</br>

#### Store several small messages

In case you want to store a lot of small messages, you can use the `BatchSend` method.
Where it is possible to store multiple messages in one entry. This will avoid creating small chunks on the server side.</br>


#### Avoid duplications

In case you want to store messages with deduplication, you need to set the producer name and the deduplication id.
See the `Deduplication` documentation for more details.</br>


#### Consumer fail over

In case you want to have a consumer fail over, you can use the `Single Active Consumer` method.
Where only one consumer is active at a time, and the other consumers are in standby mode.

#### Reliable producer and consumer

The client library provides a reliable producer and consumer, where the producer and consumer can recover from a connection failure.
See the `Reliable` documentation for more details.</br>


#### Scaling the streams

In case you want to scale the streams, you can use the `Super Stream` method.
Where you can have multiple streams and only one stream is active at a time.
See the `Super Stream` documentation for more details.</br>


#### Filtering the data when consuming

In case you want to filter the data when consuming, you can use the `Stream Filtering` method.
Where you can filter the data based on the metadata.
See the `Stream Filtering` documentation for more details.</br>


#### Using a load balancer

In case you want to use a load balancer, you can use the `Using a load balancer` method.
In Kubernetes, you can use the service name as load balancer dns.
See the `Using a load balancer` documentation for more details.</br>









