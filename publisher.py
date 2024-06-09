import logging
import pika

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

exchange_name = "amq.topic"
routing_key = "a.b.c"

while True:
    # Get user input
    user_input = '{"ABC": "ABC"}'

    # Publish the user input to the queue
    channel.basic_publish(
        exchange=exchange_name, routing_key=routing_key, body=user_input
    )

    print(f"Message '{user_input}' published to '{routing_key}' queue")
    connection.process_data_events(5)


# Close the connection
connection.close()
