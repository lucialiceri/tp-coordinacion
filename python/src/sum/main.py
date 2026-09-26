import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.closing = set() # clients I'm closing and flush cause I recived EOF
        self.flushed = set() # clients that I've already flushed, just for the eco
        self.lock = threading.Lock() 

        self.eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, ["eof"])
        self.eof_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["eof"]
        )

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.amount_by_fruit[client_id, fruit] = self.amount_by_fruit.get(
            (client_id, fruit), fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        for (c_id, fruit), final_fruit_item in self.amount_by_fruit.items():
            # Only broadcast data from client that sent EOF
            if c_id != client_id:
                continue
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [c_id, fruit, final_fruit_item.amount]
                    )
                )
        # After sending all client_id's data, delete it
        finished_keys = [k for k in self.amount_by_fruit if k[0] == client_id]
        for k in finished_keys:
            del self.amount_by_fruit[k]

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        # (client_id, fruit, amount)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)

            with self.lock:
                self.flushed.add(fields[0])
            
            self.eof_publisher.send(message)

        self._drain_closing()

        ack()

    def _drain_closing(self):
        with self.lock:
            for c_id in list(self.closing):
                self._process_eof(c_id)
                self.flushed.add(c_id)
                self.closing.remove(c_id)

    def eof_reciver(self, message, ack, nack):
        client_id_list = message_protocol.internal.deserialize(message)
        client_id = client_id_list[0]
        with self.lock:
            if client_id not in self.flushed:
                self.closing.add(client_id)
                # Thread B tells main, hey check your email! (Adds _drain_closing in the schedule)
                self.input_queue.connection.add_callback_threadsafe(self._drain_closing)
        
        ack()

    def start(self):
        t = threading.Thread(
            target=self.eof_exchange.start_consuming,
            args=(self.eof_reciver,),
        )
        t.start()
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
