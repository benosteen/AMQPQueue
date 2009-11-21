from pyinotify import *
import os
import simplejson
from amqpqueue import QueueFactory

class Log(ProcessEvent):
    def my_init(self, queue):
        self._queue = queue
    
    def _msg(self, event_type, event):
        msg = {'type':event_type, 'path':os.path.join(event.path, event.name)}
        # Why use a library for python <-> json? 
        # Makes life easier in the long run
        return simplejson.dumps(msg)
    
    # Pass on CRUD type events to the queue
    def process_IN_CREATE(self, event):
        self._queue.put(self._msg('create', event))

    def process_IN_ACCESS(self, event):
        self._queue.put(self._msg('read', event))

    def process_IN_MODIFY(self, event):
        self._queue.put(self._msg('update', event))
        
    def process_IN_DELETE(self, event):
        self._queue.put(self._msg('delete', event))

    def process_default(self, event):
        pass


qf = QueueFactory('localhost:5672')
queue = qf.Producer('inotify')

# Create inotify hook manager
wm = WatchManager()

# It is important to pass the queue!
notifier = Notifier(wm, default_proc_fun=Log(queue=queue))

wm.add_watch('/media/disk/dropbox', ALL_EVENTS)
try:
    notifier.loop()
    # Attempt to clean up after myself
except:
    pass
finally:
    queue.delete()
    queue.close()

