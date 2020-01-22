
# required packages
import os
import sys
from threading import Thread

if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())

# other modules
from video_publisher.frameANDpublish import frame_and_publish
from predicter.consumeANDpredict import consume_pred
from database import consume_save

def run(v_path, det_path, model_path, db_user, db_passwd):
    #Part 1: start frame extraction and publish to kafka topic raw
    th1 = Thread(target=frame_and_publish, args=(v_path, 'raw', 'image'), daemon=True)
    #frame_and_publish(v_path, 'raw', 'image')

    #Part 2: Consume image frames from kafka, predict, and send to kafka again
    th2 = Thread(target=consume_pred, args=('raw', 'earliest', det_path, model_path, 'display'), daemon=True)
    #consume_pred('raw', 'earliest', det_path, model_path, 'display')

    #Part 3: Grab prediction result from kafka topic display and save to db
    db_info = {
            'host': 'localhost',
            'user': db_user,
            'passwd': db_passwd,
            'db': 'sentiment'
            }
    th3 = Thread(target=consume_save, args=('display', 'earliest', db_info), daemon=True)
    #consume_save('display', 'earliest', db_info)
    # start all three tasks
    th1.start()
    th2.start()
    th3.start()

    th1.join()
    th2.join()
    th3.join()

