import traceback
import random
import time

from app.utils import EXCHANGE_SENDER_TO_CORE, ROUTING_KEY, URL
from app.utils import pika, json, uuid4, LOGGER


class Client(object):
    def __init__(self, url=URL, agent_type='TEMPERATURA', *args, **kwargs):
        self._type = self.__choose_type(agent_type, *args, **kwargs)
        self._conn = self.__conn(url, *args, **kwargs)
        self._exchange = EXCHANGE_SENDER_TO_CORE
        self._routing_key = ROUTING_KEY
        self._channel = self._conn.channel()

        self._channel.exchange_declare(exchange=self._exchange, durable=True)


    @classmethod
    def args(cls):
        from argparse import ArgumentParser

        parser = ArgumentParser()
        
        parser.add_argument("-t", "--type", default='temperatura', 
                            help="Tipo do cliente: [temperatura | humidade | luminosidade | dioxido] ")
        
        return parser.parse_args()

    def __conn(self, url, *args, **kwargs):
        return pika.BlockingConnection(pika.URLParameters(url=url))


    def __disconn(self, *args, **kwags):
        self._channel.close()

        if self._conn:
            self._conn.close()

    
    def __choose_type(self, agent_type, *args, **kwargs):
        # Colocando todos os caracteres em minúsculo:
        agent_type = agent_type.lower()

        LOGGER.info("Tipo de Agente: {}".format(agent_type))

        if agent_type == 'temperatura':
            self._factory = lambda _id: {'_id': _id, 'type': agent_type, 'message': random.randint(32,35)} 
            self._interval = 10
            return

        if agent_type == 'humidade':
            self._factory = lambda _id: {'_id': _id, 'type': agent_type, 'message': random.randint(11,15)} 
            self._interval = 20
            return

        if agent_type == 'luminosidade':
            self._factory = lambda _id: {'_id': _id, 'type': agent_type, 'message': random.randint(0, 10)} 
            self._interval = 5
            return

        if agent_type == 'dioxido':
            self._factory = lambda _id: {'_id': _id, 'type': agent_type, 'message': random.randrange(0, 1)} 
            self._interval = 30
            return


    def run(self, *args, **kwargs):
        try:
            
            while True:
                LOGGER.info("Iniciando o intervalo de frequência: {}".format(self._interval))
                
                time.sleep(self._interval)
                
                LOGGER.info("Terminando o intervalo de frequência")

                self.push(
                    self._factory( str(uuid4()) )
                    )
                
        except KeyboardInterrupt:
            self.__disconn()


    def push(self, message, *args, **kwargs):
        LOGGER.info("Publicando: {}".format(message))

        try:

            self._channel.basic_publish(
                exchange=self._exchange, 
                routing_key=self._routing_key, 
                body=json.dumps(message)
            )
        
        except Exception as e:
            print(repr(e))
            traceback.print_exc()
            raise e
       
