import threading
import time


class XThreadClass(threading.Thread):
    def __init__(self, current_name):
        super().__init__()
        self.daemon = True
        self.setName(current_name)
        print('start ' + self.getName() + ', time = ' + time.ctime() + '\n')

    def run(self):
        for i in range(10, 0, -1):
            print(self.getName() + ', step:' + str(i) + '\n')
            time.sleep(1)
        print(self.getName() + ' finished' + ', time = ' + time.ctime() + '\n')


t1 = XThreadClass('thread number 1')
t2 = XThreadClass('thread number 2')
t1.start()
t2.start()
t1.join()
t2.join()
