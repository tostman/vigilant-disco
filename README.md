# vigilant-disco

Small assignment which produces split times to Kafka and reads them and
writes to a PostgreSQL database.

### Prerequisites

Kafka and PostgreSQL instances.

### Installing

Install dependencies using pip ``pip install -r requirements.txt`` preferably
in virtualenv environment

### Deployment

Create topic with name ``timer-topic`` on Kafka instane.

Copy config file ``cp config.py.example config.py`` and put in URL:s where
Kafka and PostgreSQL are running.

Copy into ``certs`` directory ``ca.pem`` ``service.cert`` and ``service.key``
for Kafka.

Run ``./create_tables.py`` to create table into PostgeSQL database.

### Running

Start service for reading data from Kafka using ``./time_persister.py``.

Run ``./producer.py`` to create create some split times which are sent to Kafka.
