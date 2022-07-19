#!/usr/bin/env python3
"""
Module Docstring
"""
__version__ = "0.1.0"
__license__ = "MIT"

import io
import argparse
import avro.schema
import random
import time
from kafka import KafkaProducer
from avro.io import DatumWriter

SCHEMA = avro.schema.parse(open("drone-data.avsc").read())

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    while True:
        writer = DatumWriter(SCHEMA)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        time.sleep(1)
        writer.write({"x": random.uniform(0, 10), "y": random.uniform(0, 10), "z": random.uniform(0, 10), "droneId": random.randint(1, 100)}, encoder)
        producer.send('workers', bytes_writer.getvalue())


if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser() #TODO add some arguments later

    main()
