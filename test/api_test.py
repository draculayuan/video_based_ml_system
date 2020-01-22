import os
import sys
if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())
from api import *

run('/home/liamliuyuan/ml_sys/data/test2.mp4', '/home/liamliuyuan/ml_sys/util/haarcascade_frontalface_default.xml', '/home/liamliuyuan/ml_sys/util/model.h5', 'yuan', 'Liuyuan980820')
