import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time
import cv2

def consume(topic, offset):
    consumer = KafkaConsumer(topic, auto_offset_reset=offset, bootstrap_servers=['localhost:9092'], api_version=(0,10), consumer_timeout_ms=1000)
    # predict one by one and publish the predicted result
    for im in consumer:
        im = im.value
        try:
            im = np.frombuffer(im, dtype=np.int8)
            im = cv2.imdecode(im, cv2.IMREAD_COLOR)
            '''
            if im is not None:
                cv2.imwrite('test.jpg', im)
                return
            '''
            print('Processed successfully')
        except Exception as ex:
            print('failed')
            print(ex)
    consumer.close()

consume('test', 'earliest')
