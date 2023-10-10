# Consumer 

### NewConsumer()

Params: 
 - Stream name to consumer from
 - Place in the stream to start consuming from (offset)
 - Callback when a message is received


### ConsumerConfig
Fields/Functions:
 - Reference // consumer reference name
 - ClientProvidedName // CLient name used to identify consumer
 - Callback Function where consumer receives messages
 - IsSuperStream // Enable superstream consuming
 - OffsetSpec // Offset where the consumer will start consuming from
 - IsSingleActiveConsumer // when there are multiple consumers for a stream name, only one will be active at a time, the rest will be idle. 
 - ConsumerUpdateListener // rmq broker notifies a consumer that it is active before it starts dispatching messages to it. 
 - InitialCredits // The number of chunks the consumer will receive. HIgh values can increase throughput but could increase memory usage and server side CPU usage
 - Filter // only receive messages that match the filter
 - Crc32 // check crc on delivery when set. Server will send crc for each chunk.

### Consumer
Fields/Functions:
 - Autoreconnect?
 - Automatically restart consuming from the last offset
 - Handle metadata update. 
 - Handle the messages
 - Close the consumer after use


### Public Api
Create
Close

StoreOffset
Dispose?

### Private functions
ParseChunk

