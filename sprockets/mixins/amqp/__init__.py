"""The PublishingMixin wraps RabbitMQ use into a request handler, with
methods to speed the development of publishing RabbitMQ messages.

Configured using two environment variables: ``AMQP_URL`` and ``AMQP_TIMEOUT``

``AMQP_URL`` is the AMQP url to connect to, defaults to
``amqp://guest:guest@localhost:5672/%2f``.

``AMQP_TIMEOUT`` is the number of seconds to wait until timing out when
connecting to RabbitMQ.

"""
import datetime
import logging
import os
import pika
import pika.exceptions

from tornado import concurrent, gen, ioloop, locks, web

version_info = (0, 1, 4)
__version__ = '.'.join(str(v) for v in version_info)

LOGGER = logging.getLogger(__name__)


class PublishingMixin(object):
    """The request handler will connect to RabbitMQ on the first request,
    blocking until the connection and channel are established. If RabbitMQ
    closes it's connection to the app at any point, a connection attempt will
    be made on the next request.

    This class implements a pattern for the use of a single AMQP connection
    to RabbitMQ.

    Expects the :envvar:`AMQP_URL` environment variable to construct
    :class:`pika.connection.URLParameters`.

    """

    def initialize(self):
        """Initialize the RequestHandler ensuring there is an AMQP object
        associated with the application.

        """
        super(PublishingMixin, self).initialize()
        if not hasattr(self.application, 'amqp'):
            self.application.amqp = AMQP()

    @gen.coroutine
    def prepare(self):
        """Prepare the handler, ensuring RabbitMQ is connected or start a new
        connection attempt.

        If a new connection needs to be created, this will wait until the
        connection is established.

        Waiting for a new connection will timeout after ``AMQP_TIMEOUT`` and
        return a 504 error.

        """
        parent = super(PublishingMixin, self).prepare()
        if concurrent.is_future(parent):
            yield parent

        if not self._finished:
            yield self.application.amqp.maybe_connect()

    @gen.coroutine
    def amqp_publish(self, exchange, routing_key, message, properties):
        """Publish the message to RabbitMQ

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param str message: The message body
        :param dict properties: The message properties

        """
        yield self.application.amqp.publish(exchange, routing_key, message,
                                            properties)


class AMQP(object):
    """Object encapsulating all AMQP functionality"""

    DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2f'

    def __init__(self):
        self.channel = None
        self._connection = None
        self._ready = locks.Event()
        self._connecting = False
        self._timeout = datetime.timedelta(
            seconds=int(os.environ.get('AMQP_TIMEOUT', 5)))

    @gen.coroutine
    def maybe_connect(self):
        """Check and make sure that the RabbitMQ connection is established
        and if not, create it.

        """
        # The connection is established, no need to do anything
        if self._connection_is_ready:
            return

        # If not connecting, then try to connect as needed
        elif not self._connecting:
            self._connection = self._connect()

        # The connection is not established yet, wait until it is or timeout
        yield self._wait_for_connection()

    @gen.coroutine
    def publish(self, exchange, routing_key, message, properties):
        """Publish the message to RabbitMQ

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param str message: The message body
        :param dict properties: The message properties

        """
        yield self.maybe_connect()
        LOGGER.debug('Publishing to %d bytes->%s %r (Properties %r)',
                     len(message), exchange, routing_key, properties)
        self.channel.basic_publish(exchange, routing_key, message,
                                   pika.BasicProperties(**properties))

    def _connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the _on_connection_open method
        will be invoked by pika.

        :rtype: pika.TornadoConnection

        """
        LOGGER.info('Creating a new RabbitMQ connection')
        self._connecting = True
        self._ready.clear()
        return pika.TornadoConnection(self._parameters,
                                      self._on_connection_open,
                                      self._on_connection_open_error,
                                      custom_ioloop=ioloop.IOLoop.current())

    @property
    def _connection_is_ready(self):
        """Return True if the both the AMQP connection and channel are open

        :rtype: bool

        """
        return self._connection_is_open and self._channel_is_open

    @property
    def _connection_is_open(self):
        """Returns ``True``if the connection to RabbitMQ is established and
        ready to use.

        :rtype: bool

        """
        return self._connection and self._connection.is_open

    @property
    def _channel_is_open(self):
        """Returns ``True``if the connection to RabbitMQ is established and
        ready to use.

        :rtype: bool

        """
        return self.channel and self.channel.is_open

    @staticmethod
    def _connection_timeout():
        """Invoked when a connection to RabbitMQ has timed out and we
        want to return an error to the client.

        :raises: tornado.web.HTTPError

        """
        raise web.HTTPError(504, 'AMQP connection timeout')

    def _on_connection_close(self, _connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.TornadoConnection _connection: The AMQP connection object
        :param int reply_code: The code for the disconnect
        :param str reply_text: The disconnect reason

        """
        LOGGER.warning('RabbitMQ has disconnected (%s): %s, reconnecting',
                       reply_code, reply_text)
        self.channel = None
        self._connection = self._connect()

    def _on_connection_open(self, _connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it.

        :param pika.TornadoConnection _connection: The AMQP connection object

        """
        LOGGER.debug('Connected to RabbitMQ, opening a channel')
        self._connection.add_on_close_callback(self._on_connection_close)
        self._open_channel()

    def _on_connection_open_error(self, *args, **kwargs):
        """Called when RabbitMQ has been connected to.

        :raises: tornado.web.HTTPError

        """
        LOGGER.debug('Error connecting to RabbitMQ: %r, %r', args, kwargs)
        self._connecting = False
        self._ready.clear()
        raise web.HTTPError(504, 'AMQP Connection Error')

    def _open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        _on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel')
        self.channel = None
        self._connecting = True
        self._ready.clear()
        self._connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        """Called when the RabbitMQ accepts the channel open request.

        :param pika.channel.Channel channel: The channel opened with RabbitMQ

        """
        LOGGER.debug('AMQP channel opened')
        self.channel = channel
        self.channel.add_on_close_callback(self._on_channel_close)
        self._connecting = False
        self._ready.set()

    def _on_channel_close(self, channel, reply_code, reply_text):
        """Called when the RabbitMQ accepts the channel close request.

        :param pika.channel.Channel channel: The channel closed with RabbitMQ
        :param int reply_code: The code for the disconnect
        :param str reply_text: The disconnect reason

        """
        LOGGER.warning('RabbitMQ closed the channel (%s): %s',
                       reply_code, reply_text)
        if not self._connecting:
            LOGGER.info('Reopening RabbitMQ channel...')
            self._open_channel()

    @property
    def _parameters(self):
        """Return a pika URLParameters object using the :envvar:`AMQP_URL`
        environment variable which identifies the Rabbit MQ server as a URL
        ready for :class:`pika.connection.URLParameters`.

        :rtype: pika.URLParameters

        """
        return pika.URLParameters(os.environ.get('AMQP_URL', self.DEFAULT_URL))

    @gen.coroutine
    def _wait_for_connection(self):
        """Wait for a pending AMQP connection to complete"""
        try:
            yield self._ready.wait(self._timeout)
        except gen.TimeoutError:
            self._connection_timeout()
