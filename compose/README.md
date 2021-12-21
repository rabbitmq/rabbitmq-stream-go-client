RabbitMQ cluster with HA proxy 
===

how to run:

```bash
git clone git@github.com:rabbitmq/rabbitmq-stream-go-client.git .
make rabbitmq-ha-proxy 
```

ports:
```
 - localhost:5553 #standard stream port
 - localhost:5554 #TLS stream port
 - http://localhost:15673 #management port
```

RabbitMQ single node with TLS
===

```bash
git clone git@github.com:rabbitmq/rabbitmq-stream-go-client.git .
make rabbitmq-server-tls 
```

ports:
```
 - localhost:5552 #standard stream port
 - localhost:5551 #TLS stream port
 - http://localhost:15672 #management port
```