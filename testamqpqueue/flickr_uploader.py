import flickrapi

from apikeys import api_key, api_secret

if api_key == None:
    print "You need to edit the apikeys.py file to add your flickr api keys"
    import sys
    sys.exit(1)

from amqpqueue import QueueFactory

from pyworker import JSONWorker

class FlickrUploader(JSONWorker):
    def _feedback(self, progress, done):
        if done:
            print "Done uploading"
        else:
            print "At %s%%" % progress
            
    def endtask(self, msg, response):
        if ( msg.get('path','').endswith("jpg") or msg.get('path','').endswith("png") ) and msg.get('type', '') == 'create':
            filename = msg.get('path')
            f = self.context.get('flickrapi', None)
            if f:
                f.upload(filename=filename,
                         title=filename,
                         tags="magicupload",
                         is_public=1,
                         format="rest",
                         content_type=1)
            else:
                print "Failed to get flickr api"
        
        self.queue_stdin.task_done()

flickr = flickrapi.FlickrAPI(api_key, api_secret)

(token, frob) = flickr.get_token_part_one(perms='write')
if not token: raw_input("Press ENTER after you authorized this program")
flickr.get_token_part_two((token, frob))

qf = QueueFactory()

inbox = qf.Consumer('inotify')

worker = FlickrUploader(inbox, None, flickrapi=flickr)

worker.run()
