from app.utils import *

from app.publisher import Publisher
from app.consumer import Consumer


class Server(object):

    """ 
    Informações do Broker:
    """
    QUEUE_FROM_FRONT = QUEUE_FROM_FRONT
    QUEUE_TO_FRONT = QUEUE_TO_FRONT
    EXCHANGE_CORE_TO_SENDER = EXCHANGE_CORE_TO_SENDER
    EXCHANGE_SENDER_TO_CORE = EXCHANGE_SENDER_TO_CORE
    EXCHANGE_TYPE = EXCHANGE_TYPE
    ROUTING_KEY = ROUTING_KEY


    def __init__(self, amqp_url=URL):
        """
        Create a new instance of the Server, passing in the AMQP URL used to connect to RabbitMQ.

        `param` str amqp_url: The AMQP url to connect with
        """
        self._connection = None
        self._url = amqp_url

        self._consumer_tag = None

        self._consumer = Consumer(
            server_instance = self,
            queue = self.QUEUE_FROM_FRONT,
            exchange = self.EXCHANGE_SENDER_TO_CORE,
            exchange_type = self.EXCHANGE_TYPE
            )
        
        self._publisher = Publisher(
            server_instance = self,
            queue = self.QUEUE_TO_FRONT,
            exchange = self.EXCHANGE_CORE_TO_SENDER,
            exchange_type = self.EXCHANGE_TYPE
            )

        self._closing = False

    @property
    def connection(self):
        return self._connection 

    # - 1    
    def _connect(self):
        """Esse metodo retorna uma instância com a Conexão com o RabbitMq, utilizando o Tornado.
        
        Quando a conexão é estabelecida, o método on_connection_open é chamado pelo pika.
        
        :rtype: pika.SelectConnection

        """
        LOGGER.info('Abrindo Conexão no Servidor com a URL %s', self._url)
        
        return adapters.tornado_connection.TornadoConnection(pika.URLParameters(self._url),
                                                                self._on_connection_open) 

    ## - 2
    def _on_connection_open(self, unused_connection):
        """Esse método é  chamado pelo pika uma vez que a conexão com o RabbitMq é estabelecida. 
        
        Ela passa o handle para o objeto de conexão. Aqui não estamos utilizando-o.
        
        :type unused_connection: pika.SelectConnection

        """
        LOGGER.info('Conexão criada entre o RabbitMq e o Servidor.')
        self._add_on_connection_close_callback()
        self._open_channels()
    
    ### - 2.1
    def _add_on_connection_close_callback(self):
        """Esse método adiciona uma função que será chamado quando a conexão com o RabbitMq é fechada
        repentinamente.
        
        """
        LOGGER.info('Adicionando connection close callback')
        self._connection.add_on_close_callback(self._on_connection_closed)

    #### - 2.1.1
    def _on_connection_closed(self, connection, reply_code, reply_text):
        """Esse método é chamado pelo pika quando a conexão com o RabbitMq é encerrada 
        de forma repentina. Se for de fato repentina, nos reconectaremos.
        
        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._consumer._channel = None
        self._publisher._channel = None
        
        
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Conexão Fechada com o RabbitMQ; reabrindo em 5 segundos: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self._reconnect)
    
    ##### - 2.1.2
    def _reconnect(self):
        """Esse método será chamado pelo temporizador do IOLoop se a conexão for fechada. 
       
        """
        if not self._closing:
            # Criando nova Conexão
            self._connection = self._connect()

    ### - 2.2
    def _open_channels(self):
        """Abrir um novo canal com o RabbitMq emitindo o Channel.Open RPC. Quando o RabbitMQ 
        responde que o canal está aberto, a callback on_channel_open é chamado pelo pika.
       

        """
        LOGGER.info('Criando Dois novos Canais')
        
        self._connection.channel(on_open_callback=self._consumer.on_channel_open)
        self._connection.channel(on_open_callback=self._publisher.on_channel_open)

    """
    FUNÇÕES DE CONTROLE
    """
    def run(self):
        """Run the example code by connecting and then starting the IOLoop.

        """

        self._connection = self._connect()
        self._connection.ioloop.start()
    
    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info('Stopping Tornado')
        self._closing = True

        self._stop_consuming()
        
        self._connection.ioloop.stop()
        self._connection.close()

        LOGGER.info('Servidor foi Parado')

    def _stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        
        if self._consumer.channel:
            LOGGER.info("Canal do Consumidor Ativo. Fechando...")
            self._consumer.close_channel()


        if self._publisher.channel:
            LOGGER.info("Canal do Publisher Ativo. Fechando...")
            self._publisher.close_channel()


    def publish_message(self, message):
        """Função chamada pelo Consumer com o intuito de enviar uma mensagem para o Publisher. Para que
        ele, então, façã a entrega à Fila adequada.
        
        """

        self._publisher.start_publishing(message)


