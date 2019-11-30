# Implementação RabbitMQ 

Terceiro trabalho para a disciplina de Sistemas Distribuídos da Universidade Federal do Ceará.


## Início

O trabalho consiste em uma implementação de integração com o RabbitMQ. Ou seja, servidor assíncrono, utilizando Python Tornado, consumidor e clientes bloqueantes, publicadores, utilizando uma implementação básica com o `pika`.

<p align="center">
  <img src="https://ichef.bbci.co.uk/images/ic/1280xn/p07ppctk.jpg" alt="Rabbit"/>
</p>

Como solicitado na avaliação, foram desenvolvidos quatro tipos de clientes. Sendo estes escolhidos no ato da chamada do `client.py`, na raiz do projeto. 


### Instalação

Para fazer a instalação das dependências do projeto, aconselho a utilização de alguma biblioteca para **virtualização de ambientes** (p.e. conda ou virtualenv). Para instalar as dependências, execute, p.e.:

```
pip install -r requirements.txt
```

Antes de instanciar o cliente e o servidor, é necessário definir uma variável de ambiente que contenha a URL para o broker do **RabbitMQ**. P.e.:

```
export URL_AMQP=amqp://<user>:<password>@<address>
```

Feito a instalação das dependências mínimas e a definição da URL do broker, dê um *up* no Servidor:

```
python main.py
```

E um *up* no Cliente:

```
python client -t <tipo>
```


## Autores

* **Eduardo Pereira** - *Dev*
* **Bruno Mourão** - *Dev*
* **Rico João** - *Dev*

