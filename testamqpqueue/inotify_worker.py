from amqpqueue import QueueFactory
from pyworker import Worker, WorkerResponse

class MyWorker(Worker):
    def endtask(self, msg, response):
        print "message: %s" % (msg)
        self.queue_stdin.task_done()

qf = QueueFactory()

worker = MyWorker(qf.Subscriber('logger', 'inotify'))

worker.run()
