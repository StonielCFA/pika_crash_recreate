import threading
import pika
import json
import queue
import logging


class PikaThreadHandler(threading.Thread):
    def __init__(self, queue_a) -> None:
        super(PikaThreadHandler, self).__init__()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.queue_a = queue_a

    def callback_a(self, ch, method, properties, body):
        msg = body.decode('utf-8')
        result = json.loads(msg)
        self.queue_a.put(result)

    def run(self):
        exchange_name = 'amq.topic'
        routing_key = 'a.b.c'

        res = self.channel.queue_declare(queue="", exclusive=True)
        queue_name = res.method.queue
        self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback_a, auto_ack=True)

        self.channel.start_consuming()

    def stop(self):
        """Stop listening for jobs"""
        self.connection.add_callback_threadsafe(self._stop)
        self.join()

    def _stop(self):
        self.channel.stop_consuming()
        print("[PikaThreadHandler] Message consumption stopped.")


class MessageHandler:
    def __init__(self) -> None:
        self.thread = None
        self.queue_a = queue.Queue()
        self.stop = False

    def get_msg(self):
        print(f"Queue size = {self.queue_a.qsize()}")
        return self.queue_a.get()

    def stop_thread(self):
        self.thread.stop()

    def start_thread(self):
        x = PikaThreadHandler(self.queue_a)
        self.thread = x
        self.thread.start()
