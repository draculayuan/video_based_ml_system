import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time
import cv2
import os
import sys
#print(sys.path, os.getcwd())
if not os.getcwd() in sys.path:
        sys.path.append(os.getcwd())
from predicter.consumeANDpredict import consume

consume('raw', 'earliest', '/home/liamliuyuan/ml_sys/util/haarcascade_frontalface_default.xml', '/home/liamliuyuan/ml_sys/util/model.h5', 'display')
