import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time
import cv2

from consumeANDpredict import consume

consume('test', 'earliest')
