import os
import logging
import bisect
import signal
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eofs_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        top = self.fruit_top_by_client.setdefault(client_id, [])

        for i in range(len(top)):
            if top[i].fruit == fruit:
                top[i] = top[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        self.eofs_by_client[client_id] = self.eofs_by_client.get(client_id, 0) + 1
        if self.eofs_by_client[client_id] < SUM_AMOUNT:
            return
        top = self.fruit_top_by_client.get(client_id, [])
        top.sort()
        fruit_chunk = list(top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        self.fruit_top_by_client.pop(client_id, None)
        self.eofs_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def handle_sigterm(self, signum, frame):
        self.input_exchange.connection.add_callback_threadsafe(
            self.input_exchange.stop_consuming
        )

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, aggregation_filter.handle_sigterm)
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
