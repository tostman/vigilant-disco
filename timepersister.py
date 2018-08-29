from kafka import KafkaConsumer
import config

from psycopg2.extras import RealDictCursor
import psycopg2

class Database:
    def __init__(self):
        self.db_conn = psycopg2.connect(config.POSTGRESQL_SERVICE_URL)

    def create_table(self):
        sql = 'CREATE TABLE "time_line" (' + \
            '"id" serial NOT NULL PRIMARY KEY, ' + \
            '"line" text NOT NULL, ' + \
            '"number" integer NOT NULL, ' + \
            '"epoch_time" integer NOT NULL, ' + \
            '"sport_name" varchar(255) NOT NULL, ' + \
            '"sport_number" integer NOT NULL, ' + \
            '"pretty_time" varchar(255) NOT NULL, ' + \
            '"device_id" varchar(255) NOT NULL, ' + \
            '"count" integer NOT NULL);'
        c = self.db_conn.cursor(cursor_factory=RealDictCursor)
        c.execute(sql)
        self.db_conn.commit()



def kafkawip():
    consumer = KafkaConsumer(
        "timer-topic",
        bootstrap_servers=config.KAFKA_SERVICE_URL,
        client_id="demo-client-1",
        group_id="demo-group",
        security_protocol="SSL",
        ssl_cafile="certs/ca.pem",
        ssl_certfile="certs/service.cert",
        ssl_keyfile="certs/service.key",
    )

    def rp(raw_msgs):
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                print("Received: {}".format(msg.value))

    raw_msgs = consumer.poll(timeout_ms=1000);rp(raw_msgs)

if __name__ == '__main__':
    db = Database()
    db.create_table()



