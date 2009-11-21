#!/usr/bin/python
# -*- coding: utf-8 -*-

from amqpqueue import Producer, Consumer, Subscriber

class QueueFactory(object):
    """Allows you to set defaults for your producer and consumer queues
       eg
       >>> qf = QueueFactory(addr="remote:5000", exchange_name="worker_exchange")
       >>> qp = qf.Producer("my_queue")
       >>> qp.put("etc.")

       "etc." has been put to 'my_queue' on remote:5000 through the exchange "worker_exchange"

       A QueueFactory is a convenient instance to pass to a more complex daemon worker that requires
       its own queues and workers.

       The set 'defaults' can be overridden:
       >>> qp = qf.Consumer("notices", exchange_name="other_sqs_exchange")
       """
    def __init__(self, addr='localhost:5672', userid='guest', password='guest', ssl=False, exchange_name='sqs_exchange'):
        """ Sets up a context dict, so that when either a Producer or Consumer is required,
        the context can be easily overridden by supplied parameters"""
        self.context = {}
        self.context['addr'] = addr
        self.context['userid']= userid
        self.context['password']= password
        self.context['ssl'] = ssl
        self.context['exchange_name'] = exchange_name

    def Producer(self, queue, **kw):
        this_context = self.context.copy()
        for key in kw:
            this_context[key] = kw[key]
        return Producer(queue, addr=this_context['addr'],
                        userid=this_context['userid'],
                        password=this_context['password'],
                        ssl=this_context['ssl'],
                        exchange_name=this_context['exchange_name'])

    def Consumer(self, queue, **kw):
        this_context = self.context.copy()
        for key in kw:
            this_context[key] = kw[key]
        return Consumer(queue, addr=this_context['addr'],
                        userid=this_context['userid'],
                        password=this_context['password'],
                        ssl=this_context['ssl'],
                        exchange_name=this_context['exchange_name'])

    def Subscriber(self, queue, binding, **kw):
        this_context = self.context.copy()
        for key in kw:
            this_context[key] = kw[key]
        return Subscriber(queue, addr=this_context['addr'],
                        userid=this_context['userid'],
                        password=this_context['password'],
                        ssl=this_context['ssl'],
                        exchange_name=this_context['exchange_name'],
                        binding=binding)

