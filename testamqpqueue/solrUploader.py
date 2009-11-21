import solr

from amqpqueue import QueueFactory
from pyworker import JSONWorker

class SolrUploader(JSONWorker):
    def endtask(self, msg, response):
        if msg.get('path','').endswith("txt"):
            filename = msg.get('path')
            if msg.get('type', '') == 'create':
                s = self.context.get('solr','')
                if s:
                    f = open(filename, 'r')
                    blurb = f.read()
                    f.close()
                    # Encoding? who needs encoding... *cough*
                    # ... plz ignore encoding errors then...
                    s.add(id=filename, name=blurb)
                    s.commit()
            elif msg.get('type', '') == 'delete':
                s = self.context.get('solr','')
                if s:
                    s.delete(id=filename)
                    s.commit()
        self.queue_stdin.task_done()

qf = QueueFactory()

inbox = qf.Subscriber('indexer_q', 'inotify')

solr = solr.SolrConnection("http://localhost:8983/solr")

worker = SolrUploader(inbox, None, solr=solr)

worker.run()
