import logging
import multiprocessing
import os
import time
import uuid

from tornado import gen, ioloop, locks, web
import pika


LOGGER = logging.getLogger(__name__)


class AMQP(object):
    """This class encompasses all of the AMQP/RabbitMQ specific behaviors.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    DEFAULT_TIMEOUT = 10
    DEFAULT_RECONNECT_DELAY = 5
    DEFAULT_CONNECTION_ATTEMPTS = 3

    STATE_IDLE = 0x01
    STATE_CONNECTING = 0x02
    STATE_CONNECTED = 0x03
    STATE_CLOSING = 0x04
    STATE_CLOSED = 0x05
    STATE_BLOCKED = 0x06

    def __init__(self,
                 url,
                 timeout=None,
                 reconnect_delay=None,
                 connection_attempts=None,
                 on_ready_callback=None,
                 on_unavailable_callback=None,
                 on_persistent_failure_callback=None,
                 on_message_returned_callback=None,
                 io_loop=None):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str url: The AMQP URL to connect to.
        :param int timeout: The optional maximum time to wait for a bad state
                    to resolve before treating the failure as
                    persistent.
        :param int reconnect_delay: The optional time in seconds to wait before
                                    reconnecting on connection failure.
        :param int connection_attempts: The optional number of connection
                                        attempts to make before giving up.
        :param callable on_ready_callback: The optional callback to call when
                                           the connection to the AMQP server
                                           has been established and is ready.
        :param callable on_unavailable_callback: The optional callback to call
                                                 when the connection to the
                                                 AMQP server becomes
                                                 unavailable.
        :param callable on_persistent_failure_callback: The optional callback
                                                        to call when the
                                                        connection failure does
                                                        not resolve itself
                                                        within the timeout.
        :param callable on_message_returned_callback: The optional callback
                                                      to call when the AMQP
                                                      server returns a message.
        :param tornado.ioloop.IOLoop io_loop: An optional IOLoop to override
                                              the default with.

        :raises AttributeError: If timeout <= reconnect_delay

        """
        self._channel = None
        self._ioloop = ioloop.IOLoop.current(io_loop)
        self._on_ready = on_ready_callback
        self._on_unavailable = on_unavailable_callback
        self._on_persistent_failure = on_persistent_failure_callback
        self.on_message_returned = on_message_returned_callback
        self._reconnect_delay = reconnect_delay or self.DEFAULT_RECONNECT_DELAY
        self._timeout = timeout or self.DEFAULT_TIMEOUT
        self._connection_attempts = (connection_attempts or
                                     self.DEFAULT_CONNECTION_ATTEMPTS)
        self._schemas = {}
        self._state = self.STATE_IDLE
        self._url = url
        self._ready_condition = locks.Condition()

        if self._timeout <= self._reconnect_delay:
            raise AttributeError('Reconnection must be less than timeout')

        # Automatically start the RabbitMQ connection on creation
        self._connection = self.connect()

    @gen.coroutine
    def publish(self, exchange, routing_key, body,
                app_id=None, correlation_id=None, properties=None,
                mandatory=False):
        """Publish a message to RabbitMQ. If the RabbitMQ connection is not
        established or is blocked, attempt to wait until sending is possible.

        :param str exchange: The exchange to publish the message to.
        :param str routing_key: The routing key to publish the message with.
        :param bytes body: The message body to send.
        :param str app_id: The ID of the app sending the message.
        :param str correlation_id: The correlation ID associated
                                   with the message.
        :param dict properties: An optional dict of additional properties
                                to append. Will not override mandatory
                                properties:
                                app_id, correlation_id, message_id, timestamp
        :param bool mandatory: Whether to instruct the server to return an
                               unqueueable message. Default False.
                               http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish.mandatory

        """
        if not self.connected:
            LOGGER.warning('AMQP connection not ready, waiting for ready')

            ready = yield self._ready_condition.wait(
                timeout=self._ioloop.time() + self._timeout)

            if not ready:
                LOGGER.error('AMQP connection did not establish within '
                             'the timeout (%i seconds)', self._timeout)

                if self._on_persistent_failure:
                    self._on_persistent_failure(self, exchange, routing_key,
                                                body, properties, mandatory)
                else:
                    raise web.HTTPError(504, 'AMQP connection timeout')

        if self.connected:
            if not app_id:
                app_id = '{}/{}'.format(multiprocessing.current_process().name,
                                        os.getpid())
            # Set mandatory AMQP properties
            properties = properties or {}
            properties['app_id'] = app_id
            properties['correlation_id'] = correlation_id
            properties['message_id'] = str(uuid.uuid4())
            properties['timestamp'] = int(time.time())

            LOGGER.debug('Publishing %d bytes to %s %r (Properties %r)',
                         len(body), exchange, routing_key, properties)

            self._channel.basic_publish(
                exchange,
                routing_key,
                body,
                pika.BasicProperties(**properties),
                mandatory
            )

    @property
    def idle(self):
        """Returns ``True`` if the connection to RabbitMQ is closing.

        :rtype: bool

        """
        return self._state == self.STATE_IDLE

    @property
    def connecting(self):
        """Returns ``True`` if the connection to RabbitMQ is open and a
        channel is in the process of connecting.

        :rtype: bool

        """
        return self._state == self.STATE_CONNECTING

    @property
    def connected(self):
        """Returns ``True`` if the connection to RabbitMQ is connected.

        :rtype: bool

        """
        return self._state == self.STATE_CONNECTED

    @property
    def blocked(self):
        """Returns ``True`` if the connection is blocked by RabbitMQ.

        :rtype: bool

        """
        return self._state == self.STATE_BLOCKED

    @property
    def closing(self):
        """Returns ``True`` if the connection to RabbitMQ is closing.

        :rtype: bool

        """
        return self._state == self.STATE_CLOSING

    @property
    def closed(self):
        """Returns ``True`` if the connection to RabbitMQ is closed.

        :rtype: bool

        """
        return self._state == self.STATE_CLOSED

    @property
    def ready(self):
        """Returns ``True`` if the connection to RabbitMQ is established and
        we can publish to it.

        :rtype: bool

        """
        return self._state in (self.STATE_CONNECTED, self.STATE_BLOCKED)

    @property
    def channel(self):
        """Return the currently opened channel

        :rtype pika.channel.Channel: The channel open with RabbitMQ

        """
        return self._channel

    @property
    def _parameters(self):
        """Return a pika URLParameters object using the AMQP url
        which identifies the Rabbit MQ server as a URL ready for
        :class:`pika.connection.URLParameters`.

        :rtype: pika.URLParameters

        """
        parameters = pika.URLParameters(self._url)
        if self._connection_attempts > 0:
            parameters.connection_attempts = self._connection_attempts
        return parameters

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.TornadoConnection

        """
        LOGGER.debug('Connecting to %s', self._url)
        return pika.TornadoConnection(
            parameters=self._parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
            custom_ioloop=self._ioloop)

    def close(self):
        """Cleanly shutdown the connection to RabbitMQ

        """
        LOGGER.debug('Stopping')
        if not self.ready and not self.connecting:
            LOGGER.error('Closed called while not connected (%s)', self._state)
            return
        self._state = self.STATE_CLOSING
        if self._on_unavailable:
            self._on_unavailable(self)
        LOGGER.info('Closing RabbitMQ connection')
        self._connection.close()

    def _reconnect(self):
        """Schedule the next connection attempt if the class is not currently
        closing.

        """
        if self._state != self.STATE_CLOSING:
            LOGGER.debug('Attempting RabbitMQ reconnect in %s seconds',
                         self._reconnect_delay)

            def reconnect():
                """Will be invoked by the IOLoop when it is time to reconnect.

                """
                if self._state != self.STATE_CLOSING:
                    self._connection = self.connect()

            self._ioloop.call_later(self._reconnect_delay, reconnect)
        else:
            LOGGER.warning('Cowardly refusing to reconnect due to the current '
                           'object state: %s', self._state)

    """
    Connection event callbacks
    """

    def _on_connection_open(self, conn):
        """This method is called by pika once the connection to RabbitMQ has
        been established.

        :type conn: pika.TornadoConnection

        """
        LOGGER.debug('Connection opened')
        conn.add_on_connection_blocked_callback(self._on_connection_blocked)
        conn.add_on_connection_unblocked_callback(
            self._on_connection_unblocked)
        self._channel = self._open_channel()

    def _on_connection_open_error(self, _connection, error):
        """Invoked if the connection to RabbitMQ can not be made.

        :type _connection: pika.TornadoConnection
        :param Exception error: The exception indicating failure

        """
        LOGGER.critical('Could not connect to RabbitMQ: %r', error)
        self._reconnect()

    def _on_connection_blocked(self, blocked):
        """This method is called by pika if RabbitMQ sends a connection blocked
        method, to let us know we need to throttle our publishing.

        :param pika.spec.Connection.Blocked blocked: The blocked method obj

        """
        LOGGER.warning('Connection blocked: %s', blocked.reason)
        self._state = self.STATE_BLOCKED
        if self._on_unavailable:
            self._on_unavailable(self)

    def _on_connection_unblocked(self, _unblocked):
        """When RabbitMQ indicates the connection is unblocked, set the state
        appropriately.

        :param pika.spec.Connection.Unblocked _unblocked: Unblocked method obj

        """
        LOGGER.debug('Connection unblocked')
        self._state = self.STATE_CONNECTED
        if self._on_ready:
            self._on_ready(self)

    def _on_connection_closed(self, _connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.TornadoConnection _connection: Closed connection
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._connection = None
        self._channel = None
        self._state = self.STATE_CLOSED
        if self._on_unavailable:
            self._on_unavailable(self)
        if self._state != self.STATE_CLOSING:
            LOGGER.warning('Connection to RabbitMQ closed (%s): %s',
                           reply_code, reply_text)
            self._reconnect()

    """
    Channel event callbacks
    """

    def _open_channel(self):
        """Open a new channel with RabbitMQ.

        :rtype: pika.channel.Channel

        """
        LOGGER.debug('Creating a new channel')
        self._state = self.STATE_CONNECTING
        return self._connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Channel opened')
        self._state = self.STATE_CONNECTED
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._channel.add_on_flow_callback(self._on_channel_flow)
        self._channel.add_on_return_callback(self._on_message_returned)
        if self._on_ready:
            self._on_ready(self)
        self._ready_condition.notify_all()

    def _on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.

        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters.

        In this case, we just want to log the error and create a new channel
        after setting the state back to connecting.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        if self._on_unavailable:
            self._on_unavailable(self)
        self._channel = self._open_channel()

    def _on_channel_flow(self, method):
        """When RabbitMQ indicates the connection is unblocked, set the state
        appropriately.

        :param pika.spec.Channel.Flow method: The Channel flow frame

        """
        if method.active:
            LOGGER.warning('Channel flow enabled')
            self._state = self.STATE_CONNECTED
            if self._on_ready:
                self._on_ready(self)
        else:
            LOGGER.info('Channel flow disabled')
            self._state = self.STATE_BLOCKED
            if self._on_unavailable:
                self._on_unavailable(self)

    def _on_message_returned(self, channel, method, properties, body):
        """Log a returned AMQP message.

        :param pika.channel.Channel channel: The channel object
        :param pika.spec.Basic.Return method: The method object
        :param pika.spec.BasicProperties properties: The message properties
        :param str, unicode, bytes body: The message body

        """
        LOGGER.critical('%s message %s published to %s (CID %s) returned: %s',
                        method.exchange, properties.message_id,
                        method.routing_key, properties.correlation_id,
                        method.reply_text)

        if self.on_message_returned:
            self.on_message_returned(self, method, properties, body)
