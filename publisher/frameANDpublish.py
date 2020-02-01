# import packages
import logging
import cv2
import os
import sys
from kafka import KafkaProducer
from tools import setup_logger

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(dir_path)
sys.path.append(dir_path)
sys.path.append(parent_path)

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0,10))
    except Exception as ex:
        print('Exception while connecting kafka')
        print(str(ex))
    finally:
        return _producer

def frame_and_publish(topic, key, event):
    # logger
    pub_logger = setup_logger('pub_log', 'logs/publisher_log.log', logging.DEBUG)
    pub_logger.info("New publishment starts")

    producer = connect_kafka_producer()
    print('Trying to open camera')
    vidcap = cv2.VideoCapture(0)
    print('Opened camera by index 0 \n\n')
    success, image = vidcap.read()
    count = 0
    while success:
        # capture frame
        if event.is_set():
            break
        success, image = vidcap.read()
        #publish to kafka topic
        try:
            key_bytes = bytes(key, encoding='utf-8')
            ret, buffer = cv2.imencode('.jpg', image)
            producer.send(topic, key=key_bytes, value=buffer.tobytes())
            producer.flush()
            pub_logger.debug('Image {} has been published successfully'.format(count))
            count += 1
        except Exception as ex:
            pub_logger.debug('Exception in publishing message')
            pub_logger.debug(str(ex))

    # close the publisher
    if producer is not None:
        producer.close()
