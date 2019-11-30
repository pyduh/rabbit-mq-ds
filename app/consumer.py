from app.utils import LOGGER, pprint, json

class Consumer(object):
    def __init__(self, server_instance, queue, exchange, exchange_type):
        self._channel = None
        self._consumer_tag = None

        # Informações passadas na Instanciação:
        self._server = server_instance
        self._queue = queue
        self._exchange = exchange
        self._exchange_type = exchange_type

   
    # - 1   
    def on_channel_open(self, channel):
        """Este método é invocado pelo pika quando o canal foi aberto.
        O objeto do canal é passado para que possamos usá-lo.

        Como o canal agora está aberto, declaramos o exchange também.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Canal Consumer Aberto') #{}'.format(channel))
        
        self._channel = channel
        
        self._add_on_channel_close_callback(channel)
        
        # Definindo o Exchange:
        self._setup_exchange()

    ## - 1.1
    def _add_on_channel_close_callback(self, channel):
        """
        Esse método informa para o pika que o método on_channel_closed deve ser chamado quando
        o RabbitMq fecha o canal de forma inesperada.
        
        """
        LOGGER.debug('Adicionando uma callback de fechamento de canal do Consumer')
        
        channel.add_on_close_callback(self._on_channel_closed)

    ### - 1.1.1
    def _on_channel_closed(self, channel, reply_code, reply_text):
        """Chamado pelo pika quando o RabbitMq fecha inesperadamente o canal.
        
        Os canais geralmente são fechados se você tentar fazer algo que
        viola o protocolo, como declarar novamente uma troca ou fila com
        parâmetros diferentes. Neste caso, vamos fechar a conexão
        para desligar o objeto.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Canal %i foi fechado: (%s) %s', channel, reply_code, reply_text)
        
        self._server.stop()

    ## - 1.2
    def _setup_exchange(self):
        """Configure o exchange no RabbitMQ invocando o RPC Exchange.Declare
        comando. Quando estiver completo, o método on_exchange_declareok
        será invocado por pika.
       
        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declarando Exchange do Consumer: (%s - %s)'.format(self._exchange, self._exchange_type))

        self._channel.exchange_declare(self._exchange, self._exchange_type, durable=True, callback=self._on_exchange_declareok)
   
    ### - 1.2.1
    def _on_exchange_declareok(self, unused_frame):
        """Invocado pelo pika quando o RabbitMQ termina o Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange Consumer declarado')

        # Vamos agora definir a Queue:
        self._setup_queue(self._queue)
           

    #### - 1.2.1.1
    def _setup_queue(self, queue):
        """Configurando a fila no RabbitMQ chamando o RPC Queue.Declare. 
        Quando estiver completo, o método on_queue_declareok será invocado por pika
        
        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declarando Consumer Queue (%s)', queue)

        self._channel.queue_declare(queue, durable=True, callback=self._on_queue_declareok)
    
    
    def _on_queue_declareok(self, method_frame):
        """Método invocado pelo pika quando a chamada RPC Queue.Declare feita em
        setup_queue foi concluída. Neste método, vamos ligar a fila
        e exchange junto com a chave de roteamento emitindo o QueueBind
        Comando RPC. Quando este comando estiver completo, o método on_bindok
        ser invocado pelo pika.
       
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        QUEUE = method_frame.method.queue
        
        #TODO: Trocar Routing Key - Cada consumidor pode ter o seu;
        LOGGER.debug('Linkando (%s) para (%s) com (%s)',
                    self._exchange, QUEUE, self._server.ROUTING_KEY)
        
        self._channel.queue_bind(QUEUE, self._exchange, self._server.ROUTING_KEY, callback=self._on_bindok)
        
        self._start_consuming(QUEUE)

    
    def _on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info('Queue do Consumer Linkada {}'.format(unused_frame.method) )
        


    """
    PARTE REFERENTE AO CONSUMO DE MENSAGENS:
    """
    def _start_consuming(self, QUEUE):
              
        self._add_on_cancel_callback()

        self._consumer_tag = self._channel.basic_consume(QUEUE, self._on_message)


    def _add_on_cancel_callback(self):
        LOGGER.info('Adicionando callback de cancelamento de consumo')
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)


    def _on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumidor cancelado remotamente, shutting down: %r', method_frame)
        
        if self._channel:
            self._channel.close()


    def _on_message(self, unused_channel, basic_deliver, properties, body):
        """
        Chamado pelo pika quando uma mensagem é entregue a partir do RabbitMQ. O
        canal é passado. O objeto basic_deliver que é passado transporta o exchange, 
        routing key, delivery tag e um sinalizador de reenvio para a mensagem.
        
        As propriedades transmitidas são um instância de BasicProperties com as propriedades da mensagem
        e o corpo é a mensagem que foi enviada.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body
         
        """
        print("== Nova Mensagem ==")
        pprint.pprint(json.loads(body))

        # NOTE Recusa da mensagem:        
        #LOGGER.info("RECUSANDO A MENSAGEM %s", basic_deliver.delivery_tag)
        #self._non_acknowledge_message(basic_deliver.delivery_tag)
        
        # NOTE Ackeando a mensagem:
        self._acknowledge_message(basic_deliver.delivery_tag)
             

    def _acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)


    def _non_acknowledge_message(self, delivery_tag):
        LOGGER.info('Nonacknowledging message %s', delivery_tag)
        self._channel.basic_reject(delivery_tag=delivery_tag, requeue=True)


    def close_channel(self):
        LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ to close Consumer Channel')
        
        self._channel.basic_cancel(self._consumer_tag, callback=self._on_cancelok)


    def _on_cancelok(self):
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        LOGGER.info('Closing the channel')
        self._channel.close()        


    @property
    def channel(self):
        return self._channel

