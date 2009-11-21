from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name="AMQPQueue",
      version="0.4.2",
      description="AMQPQueue - Python Queue interface for AMQP",
      long_description="""\
AMQPQueue - Python Queue interface for AMQP. Works well as a queue provider for Workers constructed
from the PyWorker class
""",
      maintainer="Ben O'Steen",
      maintainer_email="bosteen@gmail.com",
      packages=find_packages(),
      install_requires=['amqplib', 'simplejson', 'httplib2'],
      )

