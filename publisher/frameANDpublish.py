# import packages
import logging
import cv2
import os
import sys
from kafka import KafkaProducer

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

def frame_and_publish(video_path, topic, key, event):
    # logger
    logging.basicConfig(filename='logs/publisher_log.txt',
                        filemode='a',
                        level=logging.DEBUG)
    pub_logger = logging.getLogger()
    pub_logger.info("New publishment starts")

    producer = connect_kafka_producer()
    vidcap = cv2.VideoCapture(video_path)
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
