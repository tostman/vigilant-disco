"""
Queue service implemented using Kafka
"""

from kafka import KafkaProducer, KafkaConsumer

import config

kafka_config = {
    "bootstrap_servers" : config.KAFKA_SERVICE_URL,
    "security_protocol" : "SSL",
    "ssl_cafile" : "certs/ca.pem",
    "ssl_certfile" : "certs/service.cert",
    "ssl_keyfile" : "certs/service.key",
    }

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(**kafka_config)

    def send(self, message):
        self.producer.send("timer-topic", message.encode("utf-8"))

    def flush(self):
        self.producer.flush()


class Consumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "timer-topic",
            client_id="demo-client-1",
            group_id="demo-group",
            **kafka_config
        )

    def get_consumer(self):
        return self.consumer
