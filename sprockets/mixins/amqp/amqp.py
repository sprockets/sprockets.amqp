"""The AMQPMixin wraps RabbitMQ use into a request handler, with
methods to speed the development of publishing RabbitMQ messages.

"""
from datetime import timedelta
from pika.adapters.tornado_connection import TornadoConnection
import logging
import os
import pika

from tornado import gen, locks, web, ioloop

LOGGER = logging.getLogger(__name__)

WAIT_SECONDS = os.environ.get('WAIT_SECONDS', 1)
WAIT_TIMEOUT = timedelta(seconds=int(WAIT_SECONDS))
AMQP_STATES = {
    'connected': 'CONNECTED',
    'connecting': 'CONNECTING',
    'disconnected': 'DISCONNECTED',
}


class AMQPMixin(object):
    """The request handler will connect to RabbitMQ on the first request,
    blocking until the connection and channel are established. If RabbitMQ
    closes it's connection to the app at any point, a connection attempt will
    be made on the next request.

    Expects the :envvar:`AMQP` environment variable to construct
    :class:`pika.connection.URLParameters`.

    https://github.com/gmr/tinman/blob/master/tinman/handlers/rabbitmq.py

    """
    amqp_ready = locks.Event()
    amqp_state = AMQP_STATES['disconnected']
    channel = None
    connection = None

    def _connect_to_rabbitmq(self):
        """Connect to RabbitMQ and assign a class attribute"""
        LOGGER.info('Creating a new RabbitMQ connection')
        custom_ioloop = ioloop.IOLoop.current()
        AMQPMixin.connection = TornadoConnection(self._rabbit_params,
                                                 self.on_conn_open,
                                                 custom_ioloop=custom_ioloop)

    def _create_new_channel(self):
        """Open a channel on the class connection"""
        AMQPMixin.connection.channel(on_open_callback=self.on_channel_open)

    def _new_message_properties(self, content_type=None, content_encoding=None,
                                headers=None, delivery_mode=None,
                                priority=None, correlation_id=None,
                                reply_to=None, expiration=None,
                                message_id=None, timestamp=None,
                                message_type=None, user_id=None, app_id=None):
        """Create a BasicProperties object, with the properties specified

        :param str content_type: MIME content type
        :param str content_encoding: MIME content encoding
        :param dict headers: Message header field table
        :param int delivery_mode: Non-persistent (1) or persistent (2)
        :param int priority: Message priority, 0 to 9
        :param str correlation_id: Application correlation identifier
        :param str reply_to: Address to reply to
        :param str expiration: Message expiration specification
        :param str message_id: Application message identifier
        :param int timestamp: Message timestamp
        :param str message_type: Message type name
        :param str user_id: Creating user id
        :param str app_id: Creating application id
        :rtype: pika.BasicProperties

        """
        return pika.BasicProperties(content_type, content_encoding, headers,
                                    delivery_mode, priority, correlation_id,
                                    reply_to, expiration, message_id,
                                    timestamp, message_type, user_id, app_id)

    def _publish_message(self, exchange, routing_key, message, properties):
        """Publish the message to RabbitMQ

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param str message: The message body
        :param pika.BasicProperties: The message properties

        """
        LOGGER.debug('Publishing message: %s to exchange: %s with '
                     'routing_key: %s and properties: %s',
                     message, exchange, routing_key, properties)
        if not self.connection or not self.channel:
            LOGGER.error('Unable to connect to Rabbit')
            raise pika.exceptions.AMQPError()
        AMQPMixin.channel.basic_publish(exchange, routing_key, message,
                                        properties)

    @property
    def _rabbit_params(self):
        """Return a pika URLParameters object using the :envvar:`AMQP`
        environment variable which identifies the Rabbit MQ server as a URL
        ready for :class:`pika.connection.URLParameters`.

        :rtype: pika.URLParameters

        """
        amqp_url = os.environ.get('AMQP',
                                  'amqp://guest:guest@localhost:5672/%2f')
        params = pika.URLParameters(amqp_url)
        LOGGER.debug('RabbitMQ connection params: %r', params)
        return params

    def on_conn_close(self, reply_code, reply_text):
        """Called when RabbitMQ has been connected to.

        :param int reply_code: The code for the disconnect
        :param str reply_text: The disconnect reason

        """
        LOGGER.warning('RabbitMQ has disconnected (%s): %s',
                       reply_code, reply_text)
        AMQPMixin.connection = None
        self.amqp_ready.clear()
        AMQPMixin.amqp_state = AMQP_STATES['disconnected']
        self._connect_to_rabbitmq()

    @gen.coroutine
    def on_conn_open(self, connection):
        """Called when RabbitMQ has been connected to.

        :param pika.connection.Connection connection: The pika connection

        """
        LOGGER.info('RabbitMQ has created a connection')
        AMQPMixin.connection = connection
        AMQPMixin.connection.add_on_close_callback(self.on_conn_close)
        self._create_new_channel()
        LOGGER.info('RabbitMQ has connected')

    def on_channel_open(self, channel):
        """Called when the RabbitMQ accepts the channel open request.

        :param pika.channel.Channel channel: The channel opened with RabbitMQ

        """
        LOGGER.info('Channel %i is opened for communication with RabbitMQ',
                    channel.channel_number)
        self.amqp_ready.set()
        AMQPMixin.channel = channel
        AMQPMixin.amqp_state = AMQP_STATES['connected']
        AMQPMixin.channel.add_on_close_callback(self.on_channel_close)

    def on_channel_close(self, channel, reply_code, reply_text):
        """Called when the RabbitMQ accepts the channel close request.

        :param pika.channel.Channel channel: The channel closed with RabbitMQ

        """
        LOGGER.info('Channel %i is closed for communication with RabbitMQ',
                    channel.channel_number)
        AMQPMixin.channel = None
        AMQPMixin.amqp_state = AMQP_STATES['disconnected']
        self._create_new_channel()

    @gen.coroutine
    def prepare(self):
        """Prepare the handler, ensuring RabbitMQ is connected or start a new
        connection attempt.

        If a new connection needs ot be created, this will wait until the
        connection is established.

        Waiting for a new connection will timeout in WAIT_SECONDS and return a
        504.

        """
        maybe_future = super(AMQPMixin, self).prepare()
        if maybe_future:
            yield maybe_future

        if self._finished:
            return

        if not self.connection and self.amqp_state != AMQP_STATES['connecting']:
            AMQPMixin.amqp_state = AMQP_STATES['connecting']
            self.amqp_ready.clear()
            try:
                self._connect_to_rabbitmq()
                yield self.amqp_ready.wait(timeout=WAIT_TIMEOUT)
            except gen.TimeoutError as e:
                self.set_status(504)
                self.finish('RabbitMQ connection timed out')
                return

        if not self.channel and self.amqp_state != AMQP_STATES['connecting']:
            AMQPMixin.amqp_state = AMQP_STATES['connecting']
            self.amqp_ready.clear()
            self._create_new_channel()
            yield self.amqp_ready.wait(timeout=WAIT_TIMEOUT)
