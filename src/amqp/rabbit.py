import json
import asyncio

import pika
from pika.exchange_type import ExchangeType
import aio_pika


class Client:
    def __init__(self, host, user, password, exchange_name, exchange_type='direct'):
        self.host = host
        self.exchange_name = exchange_name
        exchange_type_list = list(ExchangeType)
        if not exchange_type in exchange_type_list:
            raise ValueError(f'"Invalid exchange type: "{exchange_type}" not in {exchange_type_list}')
        self.exchange_type = exchange_type
        self.credentials = pika.PlainCredentials(user, password)

    def __enter__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      exchange_type=self.exchange_type,
                                      durable=False, )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def publish(self, message, routing_key=''):
        self.channel.basic_publish(exchange=self.exchange_name,
                                   routing_key=routing_key,
                                   body=json.dumps(message))

    def subscribe(self, callback, routing_key='', queue=''):
        def on_message(ch, method, properties, body):
            callback(json.loads(body.decode()))

        result = self.channel.queue_declare(queue=queue, exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.exchange_name,
                                queue=queue_name,
                                routing_key=routing_key)
        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=on_message,
                                   auto_ack=True)
        self.channel.start_consuming()



def publish(message, host, user, password, exchange_name, exchange_type='fanout', routing_key=''):
    with Client(host, user, password, exchange_name, exchange_type) as client:
        client.publish(message, routing_key)

def subscribe(callback, host, user, password, exchange_name, exchange_type='fanout', routing_key='', queue_name=''):
    with Client(host, user, password, exchange_name, exchange_type) as client:
        client.subscribe(callback, routing_key, queue_name)


# AIOClient is untested (2024-04-04)
class AIOClient:
    def __init__(self, host, user, password, exchange_name, exchange_type='direct'):
        self.host = host
        self.user = user
        self.password = password
        self.exchange_name = exchange_name
        exchange_type_list = list(ExchangeType)
        if not exchange_type in exchange_type_list:
            raise ValueError(f'"Invalid exchange type: "{exchange_type}" not in {exchange_type_list}')
        self.exchange_type = exchange_type


    async def __aenter__(self):
        conn_url = f"amqp://{self.user}:{self.password}@{self.host}/"
        self.connection = await aio_pika.connect_robust( conn_url )
        return self
    

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.connection.close()


    async def publish(self, message, routing_key=''):
        async with self.connection:
            channel = await self.connection.channel()

            exchange = await channel.declare_exchange(self.exchange_name, type=self.exchange_type)

            await exchange.publish(
                aio_pika.Message(body=json.dumps(message).encode()),
                routing_key=routing_key
            )

    async def subscribe(self, callback, routing_key='', queue_name=''):
        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with message.process():
                callback(json.loads(message.body.decode()))

        async with self.connection:
            # Creating channel
            channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=1)

            # Declaring queue
            queue = await channel.declare_queue(queue_name, exclusive=True)

            # Declaring exchange
            exchange = await channel.declare_exchange(self.exchange_name, type=self.exchange_type)

            # Binding the queue to the exchange
            await queue.bind(exchange, routing_key=routing_key)

            # Start listening the queue
            await queue.consume(on_message)

            await asyncio.Future()
