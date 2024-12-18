Go stream performances test
===

This test is to measure the performance of the stream package.

#### Install the performance test tool
To install you can download the version from GitHub:

Mac:
```
https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/latest/download/stream-perf-test_darwin_amd64.tar.gz
```

Linux:
```
https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/latest/download/stream-perf-test_linux_amd64.tar.gz
```

Windows
```
https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/latest/download/stream-perf-test_windows_amd64.zip
```

execute `stream-perf-test --help` to see the parameters. By default it executes a test with one producer, one consumer in `BatchSend` mode.

here an example:
```shell
stream-perf-test --publishers 3 --consumers 2 --streams my_stream --max-length-bytes 2GB --uris rabbitmq-stream://guest:guest@localhost:5552/  --fixed-body 400 --time 10
```

### Performance test tool Docker
A docker image is available: `pivotalrabbitmq/go-stream-perf-test`, to test it:

Run the server is host mode:
```shell
 docker run -it --rm --name rabbitmq --network host \
    rabbitmq:4-management
```
enable the plugin:
```
 docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
```
then run the docker image:
```shell
docker run -it --network host  pivotalrabbitmq/go-stream-perf-test
```

To see all the parameters:
```shell
docker run -it --network host  pivotalrabbitmq/go-stream-perf-test --help
```

### Examples

#### 1. Simple test
1 producer, 1 consumer, 1 stream, 1GB max length
```shell
stream-perf-test --streams my_stream --max-length-bytes 1GB
```

#### 2. Multiple producers and consumers
3 producers, 2 consumers, 1 stream, 2GB max length
```shell
stream-perf-test --publishers 3 --consumers 2 --streams my_stream --max-length-bytes 2GB
```

#### 3. Fixed body size
1 producer, 1 consumer, 1 stream, 1GB max length, 400 bytes body
```shell
stream-perf-test --streams my_stream --max-length-bytes 1GB --fixed-body 400
```

#### 4. Test async-send
By default, the test uses the `BatchSend` mode, to test the `Send` mode:
```shell
stream-perf-test --streams my_stream --max-length-bytes 1GB --async-send
```

#### 5. Test fixed rate and async-send
This test is useful to test the latency, the producer sends messages at a fixed rate.
```shell
stream-perf-test --streams my_stream --max-length-bytes 1GB --async-send --rate 100
```

#### 6. Batch Size

Batch Size is valid only for `Send` mode, it is the max number of messages sent in a batch.
```shell
stream-perf-test --streams my_stream --max-length-bytes 1GB --async-send --batch-size 100
```

