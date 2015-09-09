"""The RabbitMQRequestMixin wraps RabbitMQ use into a request handler, with
methods to speed the development of publishing RabbitMQ messages.

Example configuration:

    Application:
      rabbitmq:
        host: rabbitmq1
        virtual_host: my_web_app
        username: tinman
        password: tornado

"""
import os
from tornado import gen, locks
import logging
import pika
from pika.adapters import tornado_connection
from tornado import web

LOGGER = logging.getLogger(__name__)

# a datetime.timedelta relative to the current time
CHANNEL = None
CONNECTION = None
WAIT_TIMEOUT = os.environ.get('WAIT_TIMEOUT')


class RabbitMQRequestMixin(web.RequestHandler):
    """The request handler will connect to RabbitMQ on the first request,
    blocking until the connection and channel are established. If RabbitMQ
    closes it's connection to the app at any point, a connection attempt will
    be made on the next request.

    Expects the :envvar:`AMQP` environment variable to construct
    :class:`pika.connection.URLParameters`.

    https://github.com/gmr/tinman/blob/master/tinman/handlers/rabbitmq.py

    """

    def initialize(self):
        super(RabbitMQRequestMixin, self).initialize()
        self.amqp_conn_condition = locks.Condition()
        self.amqp_channel_condition = locks.Condition()

    def _connect_to_rabbitmq(self):
        """Connect to RabbitMQ and assign a class attribute"""
        global CONNECTION
        LOGGER.info('Creating a new RabbitMQ connection')
        CONNECTION = self._new_rabbitmq_connection()

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

    def _new_rabbitmq_connection(self):
        """Return a connection to RabbitMQ via the pika.Connection object.
        When RabbitMQ is connected, on_rabbitmq_open will be called.

        :rtype: pika.adapters.tornado_connection.TornadoConnection

        """
        return tornado_connection.TornadoConnection(self._rabbitmq_parameters,
                                                    self.on_conn_open)

    def _publish_message(self, exchange, routing_key, message, properties):
        """Publish the message to RabbitMQ

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param str message: The message body
        :param pika.BasicProperties: The message properties

        """
        global CHANNEL
        LOGGER.debug('Publishing message: %s to exchange: %s with '
                     'routing_key: %s and properties: %s',
                     message, exchange, routing_key, properties)
        if self._rabbitmq_is_closed or not CHANNEL:
            LOGGER.error('Unable to connect to Rabbit')
            return
        CHANNEL.basic_publish(exchange, routing_key, message, properties)

    @property
    def _rabbitmq_is_closed(self):
        """Returns True if the pika connection to RabbitMQ is closed.

        :rtype: bool

        """
        return not CONNECTION

    @property
    def _rabbitmq_parameters(self):
        """Return a pika URLParameters object using the :envvar:`AMQP`
        environment variable which identifies the Rabbit MQ server as a URL
        ready for :class:`pika.connection.URLParameters`.

        :rtype: pika.URLParameters

        """
        LOGGER.info('URLParameters: %s', pika.URLParameters(os.environ['AMQP']))
        return pika.URLParameters(os.environ['AMQP'])

    def on_conn_close(self, reply_code, reply_text):
        """Called when RabbitMQ has been connected to.

        :param int reply_code: The code for the disconnect
        :param str reply_text: The disconnect reason

        """
        global CONNECTION, CHANNEL
        LOGGER.warning('RabbitMQ has disconnected (%s): %s',
                       reply_code, reply_text)
        CONNECTION = None
        CHANNEL = None
        self._connect_to_rabbitmq()

    @gen.coroutine
    def on_conn_open(self, connection):
        """Called when RabbitMQ has been connected to.

        :param pika.connection.Connection connection: The pika connection

        """
        global CONNECTION, CHANNEL
        LOGGER.info('RabbitMQ is attempting to connect')
        CONNECTION = connection
        CONNECTION.channel(on_open_callback=self.on_channel_open)
        # wait for channel
        yield self.amqp_channel_condition.wait(timeout=WAIT_TIMEOUT)
        CHANNEL.add_on_close_callback(self.on_channel_close)
        CONNECTION.add_on_close_callback(self.on_conn_close)
        self.amqp_conn_condition.notify()
        LOGGER.info('RabbitMQ has connected')

    def on_channel_open(self, channel):
        """Called when the RabbitMQ accepts the channel open request.

        :param pika.channel.Channel channel: The channel opened with RabbitMQ

        """
        global CHANNEL
        LOGGER.info('Channel %i is opened for communication with RabbitMQ',
                    channel.channel_number)
        CHANNEL = channel
        self.amqp_channel_condition.notify()

    def on_channel_close(self, channel):
        """Called when the RabbitMQ accepts the channel close request.

        :param pika.channel.Channel channel: The channel closed with RabbitMQ

        """
        global CHANNEL
        LOGGER.info('Channel %i is closed for communication with RabbitMQ',
                    channel.channel_number)
        CHANNEL = channel

    @gen.coroutine
    def prepare(self):
        """Prepare the handler, ensuring RabbitMQ is connected or start a new
        connection attempt.

        Wait for the connection condition to notify that the rabbit connection
        has been established.

        This will timeout

        """
        maybe_future = super(RabbitMQRequestMixin, self).prepare()
        if maybe_future:
            yield maybe_future

        if self._rabbitmq_is_closed:
            self._connect_to_rabbitmq()
            yield self.amqp_conn_condition.wait(timeout=WAIT_TIMEOUT)
