from app.utils import uuid4, json, LOGGER


class Publisher(object):
    def __init__(self, server_instance, queue, exchange, exchange_type):
        self._channel = None
        self._server = server_instance
        self._queue = queue
        self._exchange = exchange
        self._exchange_type = exchange_type


        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
    

    @property
    def channel(self):
        return self._channel

    # - 1 
    def on_channel_open(self, channel):
        """Este método é invocado pelo pika quando o canal foi aberto.
        O objeto do canal é passado para que possamos usá-lo.

        Como o canal agora está aberto, declaramos o exchange também.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Canal Publisher Aberto {}'.format(channel))
        
        self._channel = channel
        
        self._add_on_channel_close_callback(channel)
        
        # Definindo o Exchange:
        self._setup_exchange()

    ## - 1.1
    def _add_on_channel_close_callback(self, channel):
        """Esse método informa para o pika que o método on_channel_closed deve ser chamado quando
        o RabbitMq fecha o canal de forma inesperada.
        
        """
        LOGGER.info('Adicionando o callback de fechamento de canal.')
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
        LOGGER.warning('Channel %i foi fechado: (%s) %s',
                       channel, reply_code, reply_text)
        
        #TODO: Enviar pro Bot de monitoramento a motivação do erro:
        message = "Reply Code %s - %s", (reply_code, reply_text)
        self._monitoring.send(application="Servidor/Publisher", message=message)
        
        self._server.connection.close()


    ## - 1.2
    def _setup_exchange(self):
        """Configure o exchange no RabbitMQ invocando o RPC Exchange.Declare
        comando. Quando estiver completo, o método on_exchange_declareok
        será invocado por pika.
       
        :param str|unicode exchange_name: The name of the exchange to declare

        """
        
        LOGGER.debug('Declarando Exchange do Producer: (%s)', self._exchange)
        
        self._channel.exchange_declare(self._exchange, self._exchange_type, durable=True, callback=self._on_exchange_declareok)
    

    ### - 1.2.1
    def _on_exchange_declareok(self, unused_frame):
        """Invocado pelo pika quando o RabbitMQ termina o Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.debug('Exchange do Publisher declarado.')

        #
        self._setup_queue(queue=self._queue)

    #### - 1.2.1.1
    def _setup_queue(self, queue):
        """
        Configurando a fila no RabbitMQ chamando o RPC Queue.Declare. 
        Quando estiver completo, o método on_queue_declareok será invocado por pika
        
        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.debug('Declarando Producer Queue (%s)', queue)

        self._channel.queue_declare(queue, durable=True, callback=self._on_queue_declareok)


    ##### - 1.2.1.1.1
    def _on_queue_declareok(self, method_frame):
        """Método invocado pelo pika quando a chamada RPC Queue.Declare feita em
        setup_queue foi concluída. Neste método, vamos ligar a fila
        e exchange junto com a chave de roteamento emitindo o QueueBind
        Comando RPC. Quando este comando estiver completo, o método on_bindok
        ser invocado pelo pika.
       
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        QUEUE = method_frame.method.queue
        
        # TODO: retirar routing key do servidor
        
        LOGGER.info('Linkando (%s) para (%s) com (%s)',
                    self._exchange, QUEUE, self._server.ROUTING_KEY)
        
        self._channel.queue_bind(QUEUE, self._server.EXCHANGE_CORE_TO_SENDER, self._server.ROUTING_KEY, callback=self._on_bindok)

    
    ###### - 1.2.1.1.1.1
    def _on_bindok(self, unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        
        LOGGER.info('Queue Publisher Bound')


    def close_channel(self):
        LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ to close Publish Channel')
        self._channel.basic_cancel("", callback=self._on_cancelok)

    def _on_cancelok(self):
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        LOGGER.info('Closing the channel')
        self._channel.close()        
            

    """
    PARTE REFERENTE AO PUBLISHER DE MENSAGEM:
    """
    def start_publishing(self, message):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        LOGGER.info('Issuing consumer related RPC commands')
        
        self._enable_delivery_confirmations()

        if self._channel is None or not self._channel.is_open:
            return # Sem canal aberto

        #hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}
        #message = u'مفتاح قيمة 键 值 キー 値'

        #Formatando de forma adequada a mensagem:
        message = dict(data=message, _id=str(uuid4()))

        LOGGER.info("Mensagem para dispachar: %s" % (message,))

        properties = pika.BasicProperties(app_id='example-publisher', content_type='application/json') #, headers=hdrs
        
        self._channel.basic_publish(self._server.EXCHANGE_CORE_TO_SENDER, self._server.ROUTING_KEY,
                                            json.dumps(message),
                                            properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        
        LOGGER.info('Mensagem Publicada # %i', self._message_number)

    def _enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self._on_delivery_confirmation)
    
    def _on_delivery_confirmation(self, method_frame):
        """Chamado pelo pika quando o RabbitMQ responde a um comando de Basic.Publish RPC,
        passando em um frame Basic.Ack ou Basic.Nack com
        a tag de entrega da mensagem que foi publicada.
        
        A tag de entrega é um contador inteiro indicando o número da mensagem que foi enviada
        no canal via Basic.Publish. 
        
        Aqui estamos apenas armazenando para acompanhar estatísticas e remover números 
        de mensagens que esperamos uma confirmação de entrega da lista usada para acompanhar as mensagens
        que estão pendentes de confirmação.

        
        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info('Recebido %s para delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        
        self._deliveries.remove(method_frame.method.delivery_tag)
        
        LOGGER.info('Published %i messages, %i have yet to be confirmed, '
                    '%i were acked and %i were nacked',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)



