import pika
import random
import string
from .middleware import MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError, MessageMiddlewareQueue, MessageMiddlewareExchange
class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        # Queue declaration
        self.queue_name = queue_name
        self.queue_declaration = self.channel.queue_declare(queue=queue_name)

        self.consumer_tag = None
        

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
 

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            # Functions needed for on_message_callback
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        # Can't start consuming without stopping first
        if self.consumer_tag is not None:
            raise MessageMiddlewareMessageError("Already consuming")
        try:
            # When a message is recieved
            self.consumer_tag =self.channel.basic_consume(
                queue = self.queue_name, 
                on_message_callback=callback
            )

            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosed) as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self):
        try:
            if self.consumer_tag:
                self.channel.basic_cancel(consumer_tag=self.consumer_tag)
                self.channel.stop_consuming()

            # Reboot variable
            self.consumer_tag = None

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosed) as e:
            raise MessageMiddlewareDisconnectedError(e)

        except Exception as e:
            raise MessageMiddlewareCloseError(e)

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()

        # Exchange declaration
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.private_queue_name = result.method.queue

        self.consumer_tag = None
        
        for key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.private_queue_name, routing_key=key)
       
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            # Functions needed for on_message_callback
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        # Can't start consuming without stopping first
        if self.consumer_tag is not None:
            raise MessageMiddlewareMessageError("Already consuming")
        try:
            # When a message is recieved
            self.consumer_tag =self.channel.basic_consume(
                queue = self.private_queue_name, 
                on_message_callback=callback
            )

            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosed) as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)


    def send(self, message):
        if not self.routing_keys:
            raise MessageMiddlewareMessageError("No routing keys configured")
        try:
             self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_keys[0], body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
       
    
    def stop_consuming(self):
        try:
            if self.consumer_tag:
                self.channel.basic_cancel(consumer_tag=self.consumer_tag)
                self.channel.stop_consuming()
            self.consumer_tag = None
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosed) as e:
            raise MessageMiddlewareDisconnectedError(e)

        except Exception as e:
            raise MessageMiddlewareCloseError(e)

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)