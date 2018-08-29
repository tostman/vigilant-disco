from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2

import config

class Database:
    def __init__(self):
        self.db_conn = psycopg2.connect(config.POSTGRESQL_SERVICE_URL)

    def run_and_commit_sql(self, sql, data=None):
        c = self.db_conn.cursor(cursor_factory=RealDictCursor)
        c.execute(sql, data)
        self.db_conn.commit()

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
        self.run_and_commit_sql(sql)

    def add_line_row(self, data):
        sql = 'INSERT INTO "time_line" VALUES (' + \
            'default, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.run_and_commit_sql(sql, data)


class Consumer:
    def __init__(self, database):
        self.database = database
        self.consumer = KafkaConsumer(
            "timer-topic",
            bootstrap_servers=config.KAFKA_SERVICE_URL,
            client_id="demo-client-1",
            group_id="demo-group",
            security_protocol="SSL",
            ssl_cafile="certs/ca.pem",
            ssl_certfile="certs/service.cert",
            ssl_keyfile="certs/service.key",
        )

    def poll(self):
        try:
            for msg in self.consumer:
                line = msg.value.decode('utf-8')
                data = (line,) + tuple(str(line).split('\t'))
                print('DATA:', len(data), data)
                self.database.add_line_row(data)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    db = Database()
    #db.create_table()
    c = Consumer(db)
    c.poll()



