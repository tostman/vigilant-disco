"""
Queue service implemented using Kafka
"""

from kafka import KafkaProducer

import config

class Queue:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_SERVICE_URL,
            security_protocol="SSL",
            ssl_cafile="certs/ca.pem",
            ssl_certfile="certs/service.cert",
            ssl_keyfile="certs/service.key",
        )

    def send(self, message):
        self.producer.send("timer-topic", message.encode("utf-8"))

    def consumer(self):
        return self.consumer

    def flush(self):
        self.producer.flush()
