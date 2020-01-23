import time
from threading import Thread, Event


def test_thread(a, b, event):
    flag = True
    while flag:
        for i in range(a+b):
            if event.is_set():
                flag = False
                break
            print(i)


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
        print('Thread termination activated!!!')


thread = ExecThread(test_thread, (5,10))
thread.start()

time.sleep(1)
thread.stop()
thread.join()
print('main programme terminated')
