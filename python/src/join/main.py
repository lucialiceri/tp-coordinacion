import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.top_by_client = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        client_id, partial_top = message_protocol.internal.deserialize(message)
        self.top_by_client.setdefault(client_id, []).append(partial_top)
        if len(self.top_by_client[client_id]) == AGGREGATION_AMOUNT:
            merged = [
                fruit_item.FruitItem(fruit, amount)
                for partial in self.top_by_client[client_id]
                for fruit, amount in partial
            ]
            merged.sort()
            fruit_top = [(fi.fruit, fi.amount) for fi in merged[-TOP_SIZE:][::-1]]
            self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
            self.top_by_client.pop(client_id)

        ack()
    
    def handle_sigterm(self, signum, frame):
        self.input_queue.connection.add_callback_threadsafe(
            self.input_queue.stop_consuming
        )

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, join_filter.handle_sigterm)
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
