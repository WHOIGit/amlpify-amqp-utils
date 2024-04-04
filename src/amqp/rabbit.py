import json
import asyncio

import pika
import aio_pika


class Client:
    def __init__(self, host, user, password, exchange_name, exchange_type='direct'):
        self.host = host
        self.user = user
        self.exchange_name = exchange_name
        assert exchange_type in list(
            pika.exchange_type.ExchangeType), f'"Invalid exchange type: "{exchange_type}" not in {list(pika.exchange_type.ExchangeType)}'
        self.exchange_type = exchange_type

        credentials = pika.PlainCredentials(self.user, password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, credentials=credentials))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      exchange_type=self.exchange_type,
                                      durable=False, )

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

    def close(self):
        self.connection.close()

def publish(message, host, user, password, exchange_name, exchange_type='fanout', routing_key=''):
    client = Client(host, user, password, exchange_name, exchange_type)
    client.publish(message, routing_key)
    client.close()

def subscribe(callback, host, user, password, exchange_name, exchange_type='fanout', routing_key='', queue_name=''):
    client = Client(host, user, password, exchange_name, exchange_type)
    client.subscribe(callback, routing_key, queue_name)  # blocking
    client.close()


# AIOClient is untested (2024-04-04)
class AIOClient:
    def __init__(self, host, user, password, exchange_name, exchange_type='direct'):
        self.host = host
        self.user = user
        self.exchange_name = exchange_name
        assert exchange_type in list(aio_pika.ExchangeType),  f'"Invalid exchange type: "{exchange_type}" not in {list(aio_pika.ExchangeType)}'
        self.exchange_type = exchange_type

        conn_url = f"amqp://{self.user}:{password}@{self.host}/"
        self.connection = await aio_pika.connect_robust( conn_url )

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
                #print(f"[x] {message.body!r}")

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

            print(" [*] Waiting for logs. To exit press CTRL+C")
            await asyncio.Future()


async def aio_publish(message, host, user, password, exchange_name, exchange_type='fanout', routing_key=''):
    client = AIOClient(host, user, password, exchange_name, exchange_type)
    await client.publish(message, routing_key)

async def aio_subscribe(callback, host, user, password, exchange_name, exchange_type='fanout', routing_key='', queue_name=''):
    client = AIOClient(host, user, password, exchange_name, exchange_type)
    await client.subscribe(callback, routing_key, queue_name)

