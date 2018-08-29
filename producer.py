#!/usr/bin/env python3

# Simple Kafka producer for sending simulated split times of competitors
# Data format is from an time taking application I have deveoped earlier
# and consists of following tab sparated values:
#  - Competitor number
#  - UNIX Epoch time
#  - Name of sport which the competitor have completed
#  - Number of sport ( 1=Swim 2=Bike 3=Run 4=Beer )
#  - Wall time in readable format
#  - Nickname of operator taking split times
#  - Index counter for times taken by operator
# e.g.
# 42	1534582420	Swim	1	11:53:40	John	2

import time

import mykafka

class TimeSender:
    def __init__(self, device_id):
        self.device_id = device_id
        self.count = 0
        self.sport_number = 1
        self.sport_name = 'Swim'
        self.queue = mykafka.Queue()

    def get_next_count(self):
        self.count += 1
        return self.count

    def send_competitor_time(self, number):
        now = time.localtime()
        epoch_time = time.strftime('%s', now)
        pretty_time = time.strftime('%H:%M:%S', now)
        count = self.get_next_count()
        message_parts = (
                str(number),
                epoch_time,
                self.sport_name,
                str(self.sport_number),
                pretty_time,
                self.device_id,
                str(count),
                )
        message = "\t".join(message_parts)
        self.queue.send(message)

    def flush(self):
        self.queue.flush()


def simulate():
    time_sender = TimeSender('to')
    for competitor in range(1, 10):
        time.sleep(3)
        time_sender.send_competitor_time(competitor)
    time_sender.flush()


if __name__ == '__main__':
    simulate()

