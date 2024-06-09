import logging
import multiprocessing
from message_handler import MessageHandler
import sys
import time


class DataIngestor:
    def __init__(self) -> None:
        fmt: str = "[%(asctime)s [%(levelname)s]: %(message)s"
        datefmt: str = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(formatter)

        root = logging.getLogger()
        root.addHandler(stream_handler)
        root.setLevel("INFO")
        self.logger = root
        logging.getLogger("pika").setLevel(logging.INFO)
        self.mh = MessageHandler()
        self.q = None

    # Start the Pika thread before multiprocessing
    def start_pika(self):
        self.mh.start_thread()
        self.q = multiprocessing.Queue(60)
        process = multiprocessing.Process(target=self.foo)
        process.start()

    def foo(self):
        while True:
            print("foo")
            time.sleep(10)

    def run(self):
        while True:
            messages = []
            while not messages:
                messages = self.mh.get_msg()
                time.sleep(1.5)
            self.logger.info(f"Got message from MH = {messages}")


def main():
    print("[INFO] Begin DataIngestor")
    di = DataIngestor()
    di.start_pika()
    di.run()
    logging.info("[INFO] end startup.py ")


if __name__ == "__main__":
    main()
