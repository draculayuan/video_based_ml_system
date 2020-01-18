import os
import sys
if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())

from video_publisher.frameANDpublish import frame_and_publish

video_path = '/home/liamliuyuan/ml_sys/data/test2.mp4'

frame_and_publish(video_path, 'raw', 'image')
