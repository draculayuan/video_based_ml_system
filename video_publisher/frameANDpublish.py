# import packages

import cv2
from kafka import KafkaProducer

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
            print('Image {} has been published successfully'.format(count))
            count += 1
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

    # close the publisher
    if producer is not None:
        producer.close()
