# amplify-amqp-utils
A pip installable package with amqp (rabbitmq) utilities for abstraction and ease of use with the AMPLIfy project.
Currently only RabbitMQ is supported, and both sync and async options are available.

## Usage
### Syncronous
```python
from amqp.rabbit import Client, publish, subscribe

def callback(msg):
    print(msg)
host='localhost'
messsage = dict(ABC=123, egg='nog')

# direct exchange
with Client(host, user='guest', password='guest', exchange_name='myDirectExchange', exchange_type='direct') as client:
    client.publish(message, routing_key='endpointA')
    client.subscribe(callback, routing_key='endpointA')  # blocking

# fanout exchange
with Client(host, user='guest', password='guest', exchange_name='myFanoutExchange', exchange_type='fanout') as client:
    client.publish(message)
    client.subscribe(callback)  # blocking

# alternative one-shot methods
publish(message, host, 'guest', 'guest', 'myDirectExchange', exchange_type='direct', routing_key='endpointB')
publish(message, host, 'guest', 'guest', 'myFanoutExchange2', exchange_type='fanout')

subscribe(callback, host, 'guest', 'guest', 'myDirectExchange', exchange_type='direct', routing_key='endpointB')  # blocking 
subscribe(callback, host, 'guest', 'guest', 'myFanoutExchange2', exchange_type='fanout')  # blocking until ^C
```

### Asyncronous
```python  
from amqp.rabbit import AIOClient, aio_publish, aio_subscribe

...

```

