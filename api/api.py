
# required packages
import os
import sys
from threading import Thread, Event

if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())
dir_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(dir_path)
sys.path.append(dir_path)
sys.path.append(parent_path)

# other modules
from publisher.frameANDpublish import frame_and_publish
from predicter.consumeANDpredict import consume_pred
from database import consume_save

class ExecThread(Thread):
    def __init__(self, func, args):
        Thread.__init__(self)
        self.stop_event = Event()
        self.func = func
        self.args = args

    def run(self):
        self.args = self.args + (self.stop_event, )
        self.func(*self.args)

    def stop(self):
        self.stop_event.set()
        print('Thread termination activated...')


class API(Thread):
    def __init__(self, det_path, model_path, db_user, db_passwd):
        Thread.__init__(self)
        self.publisher = ExecThread(frame_and_publish, ('raw', 'image'))
        self.predicter = ExecThread(consume_pred,  ('raw', 'latest', det_path, model_path, 'display'))
        db_info = {
                'host': 'localhost',
                'user': db_user,
                'passwd': db_passwd,
                'db': 'sentiment'
                }
        self.saver = ExecThread(consume_save, ('display', 'latest', db_info))
    def run(self):
        #Part 1: start frame extraction and publish to kafka topic raw
        self.publisher.start()
        #Part 2: Consume image frames from kafka, predict, and send to kafka again
        self.predicter.start()
        #Part 3: Grab prediction result from kafka topic display and save to db
        self.saver.start()
        
        # prevent main function from returning 
        self.publisher.join()
        self.predicter.join()
        self.saver.join()
        print('All threads terminated')

    def stop(self):
        print('Terminating')
        self.publisher.stop()
        self.predicter.stop()
        self.saver.stop()
