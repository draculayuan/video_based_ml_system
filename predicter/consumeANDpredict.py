#from __future__ import absolute_import

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time
import cv2
from model import get_model

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

def consume_pred(raw_topic, offset, det_path, model_path, display_topic):
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
    for im in consumer:
        im = im.value
        try:
            im = np.frombuffer(im, dtype=np.int8)
            im = cv2.imdecode(im, cv2.IMREAD_COLOR)
            # predict
            preds = detect_and_predict(detector, im, model)
            # push to kafka topic
            print('{} faces detected in current frame'.format(len(preds)))
            for p in preds:
                try:
                    p_bytes = bytes(str(p), encoding='utf-8')
                    producer.send(display_topic, key=key, value=p_bytes)
                    producer.flush()
                    print('results {} predicted and published successfully'.format(p))
                except Exception as ex:
                    print('A problem occurred when publishing to display topic')
                    print(str(ex))
        except Exception as ex:
            print('failed')
            print(ex)
    if producer is not None:
        producer.close()
    consumer.close()

