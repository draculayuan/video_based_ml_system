#from __future__ import absolute_import
import os
import sys
import logging
import numpy as np
from kafka import KafkaConsumer
import time
import pymysql

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(dir_path)
sys.path.append(dir_path)
sys.path.append(parent_path)

def consume_save(display_topic, offset, db_info, event):
    # logger
    logging.basicConfig(filename="logs/saver_log.txt",
                        filemode='a',
                        level=logging.DEBUG)
    sav_logger = logging.getLogger()
    sav_logger.info("New saving starts")

    # init db connection
    db = pymysql.connect(db_info['host'], db_info['user'], db_info['passwd'], db_info['db'])
    cursor = db.cursor()
    # consume predicted sentiment
    consumer = KafkaConsumer(display_topic, auto_offset_reset=offset, bootstrap_servers=['localhost:9092'], api_version=(0,10), consumer_timeout_ms=1000)
    # consume one by one and save the result to db
    flag = True
    num_trials = 5
    while flag:
        if num_trials == 0:
            sav_logger.debug('consumer has been idling for 5 times')
            break
        for pred in consumer:
            if event.is_set():
                flag = False
                break
            pred = pred.value
            try:
                pred = int(pred.decode("utf-8"))
                # save sentiment
                query = "INSERT INTO PRED (pred) VALUES ({})".format(pred)
                cursor.execute(query)
                db.commit()
                sav_logger.debug('Sucessfully saved sentiment {}'.format(pred))
            except Exception as ex:
                sav_logger.debug('failed')
                sav_logger(ex)
                db.rollback()
        num_trials -= 1
    consumer.close()
    db.close()

