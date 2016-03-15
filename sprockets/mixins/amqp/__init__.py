"""The PublishingMixin wraps RabbitMQ use into a request handler, with
methods to speed the development of publishing RabbitMQ messages.

Configured using two environment variables: ``AMQP_URL`` and ``AMQP_TIMEOUT``

``AMQP_URL`` is the AMQP url to connect to, defaults to
``amqp://guest:guest@localhost:5672/%2f``.

``AMQP_TIMEOUT`` is the number of seconds to wait until timing out when
connecting to RabbitMQ.

"""
import logging
import os
import pika

from tornado import gen, ioloop, locks, web

version_info = (1, 0, 0)
__version__ = '.'.join(str(v) for v in version_info)

LOGGER = logging.getLogger(__name__)


class PublishingMixin(object):
    """This mixin adds publishing messages to RabbitMQ. It uses a
    persistent connection and channel opened when the application
    start up and automatically reopened if closed by RabbitMQ

    """

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
    """Connect and maintain the connection to RabbitMQ. If RabbitMQ closes
    the connection, it will be reopened. If the channel is closed, it will
    indicate a problem with one of the commands, and will be reopened.

    :param str url: The AMQP url
    :param tornado.ioloop.IOLoop: The IOLoop to pass to pika.TornadoConnection
        If this parameter is :data:`None`, then the active IOLoop,
        as determined by :meth:`tornado.ioloop.IOLoop.instance`, is used.
    :param int timeout: The connect timeout (seconds) in the event the
        connection is closed at publish time.
    :param int connection_attempts: The maximum number of retry attempts
        when connection errors are encountered.

    """
    DEFAULT_TIMEOUT = 5
    DEFAULT_CONNECTION_ATTEMPTS = 3

    def __init__(self, url='amqp://guest:guest@localhost:5672/%2f',
                 io_loop=None, timeout=DEFAULT_TIMEOUT,
                 connnection_attempts=DEFAULT_CONNECTION_ATTEMPTS):
        self._channel = None
        self._amqp_url = url
        self._io_loop = io_loop or ioloop.IOLoop.current()
        self._timeout = timeout
        self._connnection_attempts = int(connnection_attempts) or \
            self.DEFAULT_CONNECTION_ATTEMPTS
        self._condition = locks.Condition()
        self._connecting = False
        self._connect()

    @gen.coroutine
    def publish(self, exchange, routing_key, message, properties):
        """Publish the message to RabbitMQ

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param str message: The message body
        :param dict properties: The message properties
        :raises pika.exceptions.ChannelClosed if the channel unexpectedly
            closes when pika tries to publish.
        :raises tornado.web.HTTPError if there is a timeout while trying
            to connect to RabbitMQ. Sends a 504 response.

        """
        if not self._is_ready:
            LOGGER.info('Closed channel, waiting for %s secs',
                        self._timeout)
            yield self._connection_wait()
        LOGGER.debug('Publishing %d bytes to %s %r (Properties %r)',
                     len(message), exchange, routing_key, properties)
        self._channel.basic_publish(exchange, routing_key, message,
                                    pika.BasicProperties(**properties))

    @property
    def channel(self):
        """Return the currently opened channel

        :rtype pika.channel.Channel: The channel closed with RabbitMQ

        """
        return self._channel

    def _connect(self):
        """Connects to RabbitMQ if the connection is closed"""
        if self._connecting:
            return
        self._channel = None
        self._connecting = True
        self._connection = self._open_connection()

    def _reconnect(self):
        """Reconnect to RabbitMQ if the connection is closed and
        not already connecting.

        """
        if self._connecting:
            return
        LOGGER.info('Reconnecting to %s', self._amqp_url)
        self._io_loop.add_timeout(self._io_loop.time() + 0.1, self._connect)

    def _open_connection(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the _on_connection_open method
        will be invoked by pika. In the event the connection cannot be
        opened, then the _on_connection_open_error is invoked.

        :rtype: pika.TornadoConnection

        """
        LOGGER.info('Creating connection to %s', self._amqp_url)
        return pika.TornadoConnection(
            parameters=self._parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            custom_ioloop=self._io_loop)

    @property
    def _parameters(self):
        """Return a pika URLParameters object using the AMQP url
        which identifies the Rabbit MQ server as a URL ready for
        :class:`pika.connection.URLParameters`.

        :rtype: pika.URLParameters

        """
        parameters = pika.URLParameters(self._amqp_url)
        if self._connnection_attempts > 0:
            parameters.connection_attempts = self._connnection_attempts
        return parameters

    @property
    def _is_ready(self):
        """Returns ``True``if the connection to RabbitMQ is established and
        ready to use.

        :rtype: bool

        """
        return self._channel and self._channel.is_open

    @gen.coroutine
    def _connection_wait(self):
        """Wait for a pending AMQP connection to complete"""
        ready = yield self._condition.wait(
            timeout=self._io_loop.time() + self._timeout)
        if not ready:
            raise web.HTTPError(504, 'AMQP connection timeout')

    def _on_connection_open(self, _connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it.

        :param pika.TornadoConnection _connection: The AMQP connection object

        """
        LOGGER.debug('Connected to RabbitMQ, opening a channel')
        self._connection.add_on_close_callback(self._on_connection_closed)
        self._open_channel()

    def _on_connection_closed(self, _connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.TornadoConnection _connection: The AMQP connection object
        :param int reply_code: The code for the disconnect
        :param str reply_text: The disconnect reason

        """
        LOGGER.warning('RabbitMQ has disconnected (%s): %s, reconnecting',
                       reply_code, reply_text)
        self._connecting = False
        self._connect()

    def _on_connection_open_error(self, *args, **kwargs):
        """Called when RabbitMQ has been connected to."""
        LOGGER.debug('RabbitMQ connection error: (%r), %r, reconnecting',
                     args, kwargs)
        self._connecting = False
        self._reconnect()

    def _open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open
        RPC command. When RabbitMQ responds that the channel is open,
        the _on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        """Called when the RabbitMQ accepts the channel open request.

        :param pika.channel.Channel channel: The channel opened with RabbitMQ

        """
        LOGGER.debug('RabbitMQ channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._connecting = False
        self._condition.notify_all()

    def _on_channel_closed(self, channel, reply_code, reply_text):
        """Called when the RabbitMQ accepts the channel close request.

        :param pika.channel.Channel channel: The channel closed with RabbitMQ
        :param int reply_code: The code for the disconnect
        :param str reply_text: The disconnect reason

        """
        LOGGER.warning('RabbitMQ closed the channel (%s): %s',
                       reply_code, reply_text)
        if not self._connecting:
            self._channel = None
            self._connecting = True
            self._open_channel()


def install(application, **kwargs):
    """Call this to install AMQP for the Tornado application."""
    if getattr(application, 'amqp', None) is not None:
        LOGGER.warning('AMQP is already installed')
        return False

    if os.environ.get('AMQP_TIMEOUT'):
        kwargs['timeout'] = int(os.environ['AMQP_TIMEOUT'])
    if os.environ.get('AMQP_URL'):
        kwargs['url'] = os.environ['AMQP_URL']

    setattr(application, 'amqp', AMQP(**kwargs))
    return True
