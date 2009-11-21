from amqpqueue import QueueFactory

from pyworker import Worker

import threading

import time

ADDR = 'localhost:5672'

class echoWorker(Worker):
    def endtask(self, msg, response):
        """Simple task end, ack'ing the message but printing out which thread consumed it."""
        if self.context.get('thread_id', False):
            print "From worker thread #%s\n------------------------\n%s" % (self.context.get('thread_id'), msg)
        if msg.startswith('stop'):
            self.stop = True
        self.queue_stdin.task_done()
       
class echoWorkerThread(threading.Thread):
    def __init__(self, this_id, queue_in, queue_out=None):
        self.worker = echoWorker(queue_in, queue_out, thread_id=this_id)
        threading.Thread.__init__(self)
    
    def run(self):
        # Won't bother sync'ing stdout access with a threading.Lock() atm
        self.worker.run()
        
qf = QueueFactory(addr=ADDR)

qp = qf.Producer("stdin")

threads = []
for index in xrange(0,5):
    thread = echoWorkerThread(index, qf.Consumer("stdin"))
    thread.start()
    threads.append(thread)

from uuid import uuid4


for i in xrange(0, 100):
    qp.put(uuid4().__str__())
    

for i in xrange(0, 5):
    qp.put("stop")

for thread in threads:
    thread.join()

qp.delete()
qp.close()

print "Main worker loop exited"
