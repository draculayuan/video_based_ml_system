#from __future__ import absolute_import

import numpy as np
from kafka import KafkaConsumer
import time
import pymysql

def consume_save(display_topic, offset, db_info):
    # init db connection
    db = pymysql.connect(db_info['host'], db_info['user'], db_info['passwd'], db_info['db'])
    cursor = db.cursor()
    # consume predicted sentiment
    consumer = KafkaConsumer(display_topic, auto_offset_reset=offset, bootstrap_servers=['localhost:9092'], api_version=(0,10), consumer_timeout_ms=1000)
    # consume one by one and save the result to db
    for pred in consumer:
        pred = pred.value
        try:
            pred = int(pred.decode("utf-8"))
            # save sentiment
            query = "INSERT INTO PRED (pred) VALUES ({})".format(pred)
            cursor.execute(query)
            db.commit()
            print('Sucessfully saved sentiment {}'.format(pred))
        except Exception as ex:
            print('failed')
            print(ex)
            db.rollback()
    consumer.close()
    db.close()

