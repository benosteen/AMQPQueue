from amqpqueue import QueueFactory

from pyworker import HTTPWorker

import threading

import time

ADDR = 'localhost:5672'

class echoHTTPWorker(HTTPWorker):
    def endtask(self, msg, response):
        try:
            if self.context.get('thread_id', False):
                print "From worker thread #%s" %   self.context.get('thread_id')
            first_bit = response.context['fd'].read(100)
            print "From url: %s, first 100 chars: \n %s" % (response.context['url'], first_bit)
        finally:
            response.context['fd'].close()
            if self.context.get('tempfile', False):
                remove(response.context['tempfile'])
            self.queue_stdin.task_done()

class echoHTTPWorkerThread(threading.Thread):
    def __init__(self, this_id, queue_in, queue_out=None):
        self.this_id = this_id
        self.worker = echoHTTPWorker(queue_in, queue_out, thread_id=this_id)
    
    def run(self):
        # Won't bother sync'ing stdout access with a threading.Lock() atm
        self.worker.run()
        
qf = QueueFactory(addr=ADDR)

job_queue = qf.Producer("stdin")

threads = []
for index in xrange(0,5):
    thread = echoHTTPWorkerThread(index, job_queue)
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

print "Main worker loop exited"
