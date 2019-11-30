import pika
import logging
import json
import pprint

from os import getenv
from pika import adapters
from pika.adapters.tornado_connection import TornadoConnection
from uuid import uuid4

LOG_FORMAT = "\n(%(levelname)s): (%(asctime)s) (%(filename)s:%(lineno)s)\n\t%(message)s\n"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%d-%m-%Y %H:%M:%S")
LOGGER = logging.getLogger(__name__)

QUEUE_FROM_FRONT = 'task_queue_sender_to_core'  
QUEUE_TO_FRONT = 'task_queue_core_to_sender'
EXCHANGE_CORE_TO_SENDER = 'EXCHANGE_CORE_TO_SENDER'
EXCHANGE_SENDER_TO_CORE = 'EXCHANGE_SENDER_TO_CORE'
EXCHANGE_TYPE = 'direct'
ROUTING_KEY = 'example.text'
URL = getenv('URL_AMQP')
