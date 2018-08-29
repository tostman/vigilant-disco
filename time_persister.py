#!/usr/bin/env python3

import database
import mykafka


class Consumer:
    def __init__(self):
        self.database = database.Database()
        self.consumer = mykafka.Consumer()

    def poll(self):
        try:
            for msg in self.consumer.get_consumer():
                line = msg.value.decode('utf-8')
                data = (line,) + tuple(str(line).split('\t'))
                print('DATA:', len(data), data)
                self.database.add_line_row(data)
        except KeyboardInterrupt:
            pass

    def close(self):
        self.database.close()



if __name__ == '__main__':
    c = Consumer()
    c.poll()
    c.close()



