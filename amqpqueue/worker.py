#!/usr/bin/python
# -*- coding: utf-8 -*-

# Most workers are expected to use JSON msgs
import simplejson

# For the HTTPWorker
from urllib import urlencode
import httplib2

# For tempfile/ramfile handling:
# Unicode is highly likely, therefore StringIO > cStringIO
from StringIO import StringIO
from tempfile import mkstemp
from os import remove

STATUSES = {'FAIL':0,
            'COMPLETE':1,
            }

# shorthand
FAIL = STATUSES['FAIL']
COMPLETE = STATUSES['COMPLETE']

class WorkerResponse(object):
    def __init__(self, status, **kw):
        self.status = status
        self.context = kw

class JsonMsgParseError(Exception):
    """JSON passed as a message over the queue couldn't be decoded."""
    pass

class WorkerException(Exception):
    def __init__(self, response=None):
        self.response = response
    def __str__(self):
        if self.response:
            resp_string = "Worker failed with a status: %s" % (STATUSES[self.response.status])
            if self.response.context:
                resp_string += "\n Context: %s" % self.response.context
        else:
            return "Worker failed"

class Worker(object):
    def __init__(self, queue_stdin, queue_stdout=None, **kw):
        """Base class for all the workers.
        queue_stdin - the instance passed through queue_stdin should implement a 
        blocking .get(), .task_done() and __len__().
        queue_stdout - the instance passed should implement a blocking .put() and
        non-blocking __len__()
        Other keyword parameters can be passed to the workers as necessary.
        
        Overwrite the .starttask(msg) command, which is passed the contents of the message.
        
        .endtask(msg, response, **kw) can likewise be overridden to perform addition tasks
        after the main work has been completed - BUT methods must acknoledge the msg via a
        .task_done() on the queue "stdin".
        
        self.context is a dictionary of all other parameters passed to the worker.
        """
        self.queue_stdin = queue_stdin
        self.queue_stdout = queue_stdout
        self.context = kw
        self.stop = False
        if 'start' in kw:
            self.run()
    
    def parse_json_msg(self, msg, encoding="UTF-8"):
        try:
            return simplejson.loads(msg,encoding=encoding)
        except:
            raise JsonMsgParseError
    
    def run(self):
        while (True):
            # Blocking call:
            if self.stop:
                break
            msg = self.queue_stdin.get()
            # TODO implement variable timeout on .starttask() method
            resp = self.starttask(msg)
            self.endtask(msg, resp)
            
    def starttask(self, msg):
        """Implements a basic 'echo' worker - pointless, but illustrative.
        This method should be overridden by a specific worker class."""
        return WorkerResponse(COMPLETE)

    def endtask(self, msg, response):
        """Simple task end, ack'ing the message consuming it on a COMPLETE response."""
        if response.status == FAIL:
            raise WorkerException(resp)
        elif response.status == COMPLETE:
            if self.queue_stdout:
                self.queue_stdout.put(msg)
            else:
                # print msg
                pass
            self.queue_stdin.task_done()

class JSONWorker(Worker):
    """Similar in practice to the normal Worker, except it only tolerates JSON
    messages and will ignore any it cannot parse.
    Passing it an outbound queue will allow it to pass any unparsable msgs onwards."""
    def run(self):
        while (True):
            # Blocking call:
            if self.stop:
                break
            msg = self.queue_stdin.get()
            # TODO implement variable timeout on .starttask() method
            try:
                jmsg = simplejson.loads(msg)
                resp = self.starttask(jmsg)
                self.endtask(jmsg, resp)
            except Exception, e:
                print "Failed to parse\n%s" % msg
                print e
                if self.queue_stdout:
                    self.queue_stdout.put(msg)
                # Actively consume bad messages
                self.queue_stdin.task_done()

class WorkerFactory(object):
    def get(config):
        pass

class HTTPWorker(Worker):
    """Gets a local copy of the resource at the URL in the JSON msg ('url') and simply
    prints the first "line".
    
    It is expected that self.endtask will be overwritten. 
    
    If the tempfile option is set, remember to delete the temporary file 
    as well as ack the msg! Eg -
    ------------------------------------------------------------
    import os
    class SolrFeeder(HTTPWorker):
        def endtask(self, msg, response):
            try:
                # do stuff with response.context['fd'], the file-descriptor for the resource
            finally:
                response.context['fd'].close()
                if self.context.get('tempfile', False):
                   os.remove(response.context['tempfile'])
                self.queue_stdin.task_done()

    s = SolrFeeder(queue_stdin, queue_stdout=None, tempfile = True)
    ------------------------------------------------------------    
    If 'id' is passed in the message instead, then this is inserted into a template, set
    by instantiating this worker with the parameter 'http_template'. Normal python
    string formating applies ( template % id )
    
    Requires configuration parameters:
        http_template = template for the URL to GET
        """
    def _get_tempfile(self):
        return mkstemp()
    
    def _get_ramfile(self):
        return (StringIO(), None)
    
    def httpsetup(self):
        self.http_template = self.context.get('http_template', None)
        self.h = httplib2.Http()
        self.method = self.context.get('method', 'GET')
        self.data_method = self.context.get('method', 'GETURL')
        if self.context.get('tempfile', False):
            self.tempfile = self._get_tempfile
        else:
            self.tempfile = self._get_ramfile
            
        self.setup = True
    
    def starttask(self, msg):
        """This will very simply GET the url supplied and pass the temp/ramfile to endtask"""
        try:
            if not self.setup:
                self.httpsetup()
            (fd, name) = self.tempfile()
            jmsg = self.parse_json_msg(msg)
            # Prepare HTTP request
            headers = {}
            if 'headers' in jmsg:
                headers = jmsg['headers']
            url = None
            if 'url' in jmsg:
                url = jmsg['url']
            elif 'id' in jmsg and self.http_template:
                url = self.http_template % jmsg['id']
            else:
               return WorkerResponse(FAIL)
            if not url:
                raise Exception("url not supplied")
            fd.write(h.request(jmsg['url'], "GET", headers=headers))
            fd.seek(0)
            return WorkerResponse(COMPLETE, fd=fd, tempfile=name, jmsg=jmsg, url=url)
        except Exception, e:
            return WorkerResponse(FAIL, exception = e)

    def endtask(self, msg, response):
        """Demo method to be overwritten. This simply reads the first 100 characters from
        the reponse.context['fd'] (file-handle) and deletes/removes the file."""
        try:
            first_bit = response.context['fd'].read(100)
            if self.queue_stdout:
                self.queue_stdout.put(first_bit)
            else:
                print "From url: %s, first 100 chars: \n %s" % (response.context['url'], first_bit)
        finally:
            response.context['fd'].close()
            if self.context.get('tempfile', False):
                remove(response.context['tempfile'])
            self.queue_stdin.task_done()
            

class ShellFileWorker(Worker):
    pass
class ShellWorker(Worker):
    pass
