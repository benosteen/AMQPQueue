try:
    import pkg_resources
    pkg_resources.require('amqplib >= 0.5')
except ImportError:
    pass

import amqplib.client_0_8 as amqp
import logging

try:
    import cPickle as pickle
except ImportError:
    import pickle


logging.getLogger('amqplib').setLevel(logging.INFO) # silence amqplib
log = logging.getLogger('amqpqueue')



class Error(Exception):
    "Exception raised by AmqpQueue.get()"
    pass

class _AmqpQueue:
    '''From http://www.lshift.net/blog/2009/06/11/python-queue-interface-for-amqp

    This module attempts to create a simple wrapper to Amqp.
    The idea is to mimic the subset of python Queue interface.

        - the queue is persistent
        - no message in queue can be lost
        - message is delivered once (not fanout/broadcast)


    Just to remind, the AMQP model:

    exchange  --->  binding  ---->  queue  --->  consumer#1
                                           --->  consumer#2

    >>> import threading

    >>> FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
    >>> logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)

    >>> qp = Producer('test_q')
    >>> qp.put('test')
    >>> qc = Consumer('test_q')
    >>> qc.get()
    'test'
    >>> qc.get()
    Traceback (most recent call last):
     ...
    Error: You must call queue.task_done before you are allowed to get new item.
    >>> qc.task_done()
    >>> len(qp)
    0
    >>> qp.consumers()
    1
    >>> threading.Timer(1.0, lambda: qp.put('a') ).start()
    >>> qc.get()
    'a'
    >>> qc.task_done()
    >>> qc.delete(); qc.close()
    >>> qp.delete(); qp.close()
    >>> qc = Consumer('test_qas')
    >>> qc.delete(); qc.close()
    '''
    # pickle load/dump
    dumps = lambda _,s:pickle.dumps(s, -1)
    loads = pickle.loads
    content_type = "text/x-python"

    def __init__(self, queue_name, addr='localhost:5672', \
                        userid='guest', password='guest', ssl=False, exchange_name='sqs_exchange', binding=None):
        self.addr = addr
        self.queue_name = queue_name
        if binding:
            self.binding = binding
        else:
            self.binding = queue_name
        self.exchange_name = exchange_name
        self.addr = addr
        self.userid = userid
        self.password = password
        self.ssl = ssl

        ''' Create amqp connection, channels and bindings '''
        self.conn = amqp.Connection(self.addr,
                                    userid = self.userid,
                                    password = self.password,
                                    ssl = self.ssl)
        self.ch = self.conn.channel()

    def _close_connection(self):
        ''' Drop tcp/ip connection and amqp abstractions '''
        for obj in [self.ch, self.conn, self.conn.transport.sock]:
            try:
                obj.close()
            except Exception:
                pass

        self.ch, self.conn = None, None

    def _declare(self):
        ''' Define amqp queue, returns (qname, n_msgs, n_consumers) '''
        return self.ch.queue_declare(self.queue_name, passive=False, \
                            durable=True, exclusive=False, auto_delete=False)

    def qsize(self):
        ''' Return number of messages waiting in this queue '''
        _, n_msgs, _ = self._declare()
        return n_msgs

    def consumers(self):
        ''' How many clients are currently listening to this queue. '''
        _, _, n_consumers = self._declare()
        return n_consumers

    def __len__(self):
        ''' I think Queue should support len()  '''
        return self.qsize()

    def delete(self):
        ''' Delete a queue and free data tied to it. '''
        try:
            self.ch.queue_delete(self.queue_name)
        except (TypeError, amqp.AMQPChannelException):
            pass

    def close(self):
        ''' Close tcp/ip connection '''
        self._close_connection()


class Producer(_AmqpQueue):
    '''
    Creates/sends/produces messages into the queue.
    '''
    def __init__(self, *args, **kwargs):
        _AmqpQueue.__init__(self, *args, **kwargs)
        self.ch.access_request('/data', active=True, read=False, write=True)
        self.ch.exchange_declare(self.exchange_name, 'direct', \
                                                durable=True, auto_delete=False)
        self._declare()
        self.ch.queue_bind(self.queue_name, self.exchange_name, self.queue_name)

    def put(self, message):
        ''' Add message to queue '''
        msg = amqp.Message(self.dumps(message), content_type=self.content_type)
        self.ch.basic_publish(msg, self.exchange_name, self.queue_name)


class Consumer(_AmqpQueue):
    '''
    Receives/consumes messages from the queue.
    '''
    def __init__(self, *args, **kwargs):
        _AmqpQueue.__init__(self, *args, **kwargs)
        self.ch.access_request('/data', active=True, read=True, write=False)
        self._declare()
        self.ch.queue_bind(self.queue_name, self.exchange_name, self.queue_name)
        self.delivery_tag = None

        self.consumer_tag = self.ch.basic_consume(self.queue_name,
                                            callback=self._amqp_callback)
        self._amqp_messages = []

    def get(self):
        """
        Timeout and non-blocking is not implemented.
        """
        if self.delivery_tag is not None:
            raise Error('You must call queue.task_done'
                                 ' before you are allowed to get new item.')


        msg = self._get_blocking()

        data = self.loads(msg.body)
        self.delivery_tag = msg.delivery_tag
        return data

    def _amqp_callback(self, msg):
        self._amqp_messages.append(msg)

    def _get_blocking(self):
        while not self._amqp_messages:
            self.ch.wait()

        return self._amqp_messages.pop(0)

    def task_done(self):
        ''' Indicate that a formerly enqueued task is complete. '''
        assert self.delivery_tag is not None
        self.ch.basic_ack(self.delivery_tag)
        self.delivery_tag = None

    def task_failed(self):
        ''' Indicate that a formerly enqueued task has failed. This will return the
        msg to the queue.'''
        assert self.delivery_tag is not None
        self.ch.basic_reject(self.delivery_tag, requeue=True)
        self.delivery_tag = None


class Subscriber(Consumer):
    '''
    Receives/consumes messages from a subscription queue. If 3 Subscribers connect to
    a given Producer, each will have it's own persistent queue, and each queue would
    recieve a copy of any message the Producer puts out (Fan-out.)
    '''
    def __init__(self, *args, **kwargs):
        _AmqpQueue.__init__(self, *args, **kwargs)
        self.ch.access_request('/data', active=True, read=True, write=False)
        self._declare()
        self.ch.queue_bind(self.queue_name, self.exchange_name, self.binding)
        self.delivery_tag = None

        self.consumer_tag = self.ch.basic_consume(self.queue_name,
                                            callback=self._amqp_callback)
        self._amqp_messages = []



if __name__ == '__main__':
    import sys
    import doctest
    try:
        import coverage
    except ImportError:
        print >> sys.stderr, " [*] python-coverage not found"
        coverage = None

    if coverage:
        coverage.erase()
        coverage.start()
        coverage.exclude('#pragma[: ]+[nN][oO] [cC][oO][vV][eE][rR]')


    import amqpqueue
    modules = [amqpqueue]
    for module in modules:
        doctest.testmod(module)

    if coverage:
        coverage.stop()
        coverage.report(modules, ignore_errors=1, show_missing=1)
        coverage.erase()

