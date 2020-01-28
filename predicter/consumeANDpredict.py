#from __future__ import absolute_import

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time
import cv2
import os
import sys
import logging
from model import get_model
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

def detect_and_predict(detector, im, model):
    gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    faces = detector.detectMultiScale(gray, scaleFactor=1.3, minNeighbors=5)
    # crop and resize
    results = []
    for (x, y, w, h) in faces:
        roi = gray[y:y+h, x:x+w]
        cropped_img = np.expand_dims(np.expand_dims(cv2.resize(roi, (48,48)), -1),0)
        pred = model.predict(cropped_img)
        pred = int(np.argmax(pred))
        results.append(pred)
    return results

def consume_pred(raw_topic, offset, det_path, model_path, display_topic, event):
    # logger
    pre_logger = setup_logger('pre_log', 'logs/predicter_log.log', logging.DEBUG)
    pre_logger.info("New prediction starts")

    # producer to publish to display topic
    producer = connect_kafka_producer()
    # face detector to crop faces
    detector = cv2.CascadeClassifier(det_path)
    # load sentiment prediction model
    model = get_model()
    model.load_weights(model_path)
    # consume raw images
    consumer = KafkaConsumer(raw_topic, auto_offset_reset=offset, bootstrap_servers=['localhost:9092'], api_version=(0,10), consumer_timeout_ms=1000)
    key = bytes('display', encoding='utf-8')
    # predict one by one and publish the predicted result
    flag = True
    num_trials = 5
    while flag:
        if num_trials == 0:
            pre_logger.debug('Consumer has been idling for 5 times')
            break
        for im in consumer:
            # terminate
            if event.is_set():
                # quit two layers of loop
                flag = False
                break
            im = im.value
            try:
                im = np.frombuffer(im, dtype=np.int8)
                im = cv2.imdecode(im, cv2.IMREAD_COLOR)
                # predict
                preds = detect_and_predict(detector, im, model)
                # push to kafka topic
                pre_logger.debug('{} faces detected in current frame'.format(len(preds)))
                for p in preds:
                    try:
                        p_bytes = bytes(str(p), encoding='utf-8')
                        producer.send(display_topic, key=key, value=p_bytes)
                        producer.flush()
                        pre_logger.debug('results {} predicted and published successfully'.format(p))
                    except Exception as ex:
                        pre_logger.debug('A problem occurred when publishing to display topic')
                        pre_logger.debug(str(ex))
            except Exception as ex:
                pre_logger.debug('failed')
                pre_logger.debug(ex)

            num_trials -= 1
    if producer is not None:
        producer.close()
    consumer.close()

