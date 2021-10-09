Stream examples
===

 - [Getting started](./getting_started.go). A good point to start.
 - [Offset Start](./offsetStart/offset.go). How to set different points to start consuming
 - [Offset Tracking](./offsetTracking/offsetTracking.go). How to store the consumer position server side
 - [Getting started TLS](./tls/getting_started_tls.go). A TLS example.
 - [HA Producer](./haProducer/producer.go). HA producer example (Still experimental)
 - [Deduplication](./deduplication/deduplication.go). deduplication example, run it more than one time, and the records <br />
   won't change, since the server will handle the deduplication.
 - [Using a load balancer](./proxy/proxy.go). An example how to use the client with a TLS load balancer.<br />
   To run the example run the [RabbitMQ TLS cluster](../compose). <br />
   For more details: https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/
