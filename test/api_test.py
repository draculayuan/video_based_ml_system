import os
import sys
if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())
from api import *
from threading import Thread

api = API('/home/liamliuyuan/ml_sys/data/test2.mp4', '/home/liamliuyuan/ml_sys/util/haarcascade_frontalface_default.xml', '/home/liamliuyuan/ml_sys/util/model.h5', 'yuan', 'Liuyuan980820')

import time

p = Thread(target = api.run)
p.start()
time.sleep(1)
api.stop()
p.join()
