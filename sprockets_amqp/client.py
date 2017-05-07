import logging
import time
import uuid

from tornado import concurrent, ioloop
import pika.exceptions

from sprockets_amqp import exceptions


STATE_IDLE = 1
STATE_CONNECTING = 2
STATE_READY = 3
STATE_BLOCKED = 4
STATE_CLOSING = 5
STATE_CLOSED = 6
STATE_DESC = {STATE_IDLE: 'Idle',
              STATE_CONNECTING: 'Connecting',
              STATE_READY: 'Ready',
              STATE_BLOCKED: 'Blocked',
              STATE_CLOSING: 'Closing',
              STATE_CLOSED: 'Closed'}

DEFAULT_RECONNECT_DELAY = 5
DEFAULT_CONNECTION_ATTEMPTS = 3


class Client(object):
    """This class encompasses all of the AMQP/RabbitMQ specific behaviors.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self,
                 url,
                 enable_confirmations=True,
                 reconnect_delay=DEFAULT_RECONNECT_DELAY,
                 connection_attempts=DEFAULT_CONNECTION_ATTEMPTS,
                 default_app_id=None,
                 on_ready_callback=None,
                 on_unavailable_callback=None,
                 on_return_callback=None,
                 io_loop=None):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str url: The AMQP URL to connect to
        :param bool enable_confirmations: Enable Publisher Confirmations
        :param int reconnect_delay: The optional time in seconds to wait before
            reconnecting on connection failure.
        :param int connection_attempts: The optional number of connection
            attempts to make before giving up.
        :param str default_app_id: The default AMQP application ID
        :param callable on_ready_callback: The optional callback to call when
            the connection to RabbitMQ has been established and is ready.
        :param callable on_unavailable_callback: The optional callback to call
            when the connection to the AMQP server becomes unavailable.
        :param callable on_return_callback: The optional callback
            that is invoked if a message is  returned because it is unroutable
        :param tornado.ioloop.IOLoop io_loop: An optional IOLoop to override
            the default with.
        :raises: ValueError

        """
        if not int(connection_attempts):
            raise ValueError(
                'Invalid connection_attempts value: {}'.format(
                    connection_attempts))

        if not float(reconnect_delay):
            raise ValueError(
                'Invalid reconnect_delay value: {}'.format(reconnect_delay))

        self.logger = logging.getLogger('sprockets_amqp.Client')
        self.state = STATE_IDLE
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.channel = None
        self.connection = None
        self.connection_attempts = int(connection_attempts)
        self.default_app_id = default_app_id
        self.message_number = 0
        self.messages = {}
        self.on_ready = on_ready_callback
        self.on_return = on_return_callback
        self.on_unavailable = on_unavailable_callback
        self.publisher_confirmations = enable_confirmations
        self.reconnect_delay = float(reconnect_delay)
        self.url = url
        self.parameters = pika.URLParameters(url)
        self.parameters.connection_attempts = self.connection_attempts

        # Automatically start the RabbitMQ connection on creation
        self.connect()

    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ. If the RabbitMQ connection is not
        established or is blocked, attempt to wait until sending is possible.

        :param str exchange: The exchange to publish the message to.
        :param str routing_key: The routing key to publish the message with.
        :param bytes body: The message body to send.
        :param dict properties: An optional dict of additional properties
                                to append.
        :rtype: tornado.concurrent.Future
        :raises: :exc:`sprockets_amqp.exceptions.NotReadyError`
        :raises: :exc:`sprockets_amqp.exceptions.PublishingFailure`

        """
        future = concurrent.Future()

        properties = properties or {}
        properties.setdefault('app_id', self.default_app_id)
        properties.setdefault('message_id', str(uuid.uuid4()))
        properties.setdefault('timestamp', int(time.time()))

        if self.ready:
            if self.publisher_confirmations:
                self.message_number += 1
                self.messages[self.message_number] = future
            else:
                future.set_result(None)

            try:
                self.channel.basic_publish(
                    exchange, routing_key, body,
                    pika.BasicProperties(**properties), True)
            except pika.exceptions.AMQPError as error:
                future.set_exception(
                    exceptions.PublishingFailure(
                        properties['message_id'],
                        exchange, routing_key,
                        error.__class__.__name__))
        else:
            future.set_exception(exceptions.NotReadyError(
                self.state_description, properties['message_id']))
        return future

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        self.logger.debug('Received %s for delivery tag: %i',
                          confirmation_type, method_frame.method.delivery_tag)

        if method_frame.method.multiple:
            confirmed = sorted([msg for msg in self.messages
                                if msg <= method_frame.method.delivery_tag])
        else:
            confirmed = [method_frame.method.delivery_tag]

        for msg in confirmed:
            self.logger.debug('RabbitMQ confirmed message %i', msg)
            try:
                if confirmation_type == 'ack':
                    self.messages[msg].set_result(None)
                elif confirmation_type == 'nack':
                    self.messages[msg].set_exception(
                        exceptions.PublishingFailure(msg))
            except KeyError:
                self.logger.warning(
                    'Tried to confirm a message missing in stack')
            else:
                del self.messages[msg]

        self.logger.debug('Published %i messages, %i have yet to be confirmed',
                          self.message_number, len(self.messages))

    @property
    def idle(self):
        """Returns :data:`True` if the connection to RabbitMQ is closing.

        :rtype: bool

        """
        return self.state == STATE_IDLE

    @property
    def connecting(self):
        """Returns :data:`True` if the connection to RabbitMQ is open and a
        channel is in the process of connecting.

        :rtype: bool

        """
        return self.state == STATE_CONNECTING

    @property
    def blocked(self):
        """Returns :data:`True` if the connection is blocked by RabbitMQ.

        :rtype: bool

        """
        return self.state == STATE_BLOCKED

    @property
    def closable(self):
        """Returns :data:`True` if the connection to RabbitMQ can be closed

        :rtype: bool

        """
        return self.state in [STATE_BLOCKED, STATE_READY]

    @property
    def closed(self):
        """Returns :data:`True` if the connection to RabbitMQ is closed.

        :rtype: bool

        """
        return self.state == STATE_CLOSED

    @property
    def closing(self):
        """Returns :data:`True` if the connection to RabbitMQ is closing.

        :rtype: bool

        """
        return self.state == STATE_CLOSING

    @property
    def ready(self):
        """Returns :data:`True` if the connection to RabbitMQ is established
        and we can publish to it.

        :rtype: bool

        """
        return self.state == STATE_READY

    @property
    def state_description(self):
        """Return the human understandable state description.

        :rtype: str

        """
        return STATE_DESC[self.state]

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.adapters.tornado_connection.TornadoConnection

        """
        if not self.idle and not self.closed:
            raise exceptions.ConnectionStateError(self.state_description)
        self.logger.debug('Connecting to %s', self.url)
        self.state = STATE_CONNECTING
        self.connection = pika.TornadoConnection(
            parameters=self.parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
            custom_ioloop=self.io_loop)

    def close(self):
        """Cleanly shutdown the connection to RabbitMQ

        :raises: sprockets_amqp.exceptions.ConnectionStateError

        """
        if not self.closable:
            self.logger.warning('Closed called while %s',
                                self.state_description)
            raise exceptions.ConnectionStateError(self.state_description)
        self.state = STATE_CLOSING
        self.logger.info('Closing RabbitMQ connection')
        self.connection.close()

    def _open_channel(self):
        """Open a new channel with RabbitMQ.

        :rtype: pika.channel.Channel

        """
        self.logger.debug('Creating a new channel')
        return self.connection.channel(self.on_channel_open)

    def _reconnect(self):
        """Schedule the next connection attempt if the class is not currently
        closing.

        """
        if self.idle or self.closed:
            self.logger.debug('Attempting RabbitMQ reconnect in %s seconds',
                              self.reconnect_delay)
            self.io_loop.call_later(self.reconnect_delay, self.connect)
            return
        self.logger.warning('Reconnect called while %s',
                            self.state_description)

    #
    # Connection event callbacks
    #

    def on_connection_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established.

        :type connection: pika.adapters.tornado_connection.TornadoConnection

        """
        self.logger.debug('Connection opened')
        connection.add_on_connection_blocked_callback(
            self.on_connection_blocked)
        connection.add_on_connection_unblocked_callback(
            self.on_connection_unblocked)
        connection.add_backpressure_callback(self.on_back_pressure_detected)
        self.channel = self._open_channel()

    def on_connection_open_error(self, connection, error):
        """Invoked if the connection to RabbitMQ can not be made.

        :param pika.adapters.tornado_connection.TornadoConnection connection:
        :param Exception error: The exception indicating failure

        """
        self.logger.critical('Could not connect to RabbitMQ (%s): %r',
                             connection, error)
        self.state = STATE_CLOSED
        self._reconnect()

    def on_back_pressure_detected(self, obj):  # pragma: nocover
        """This method is called by pika if it believes that back pressure is
        being applied to the TCP socket.

        :param unknown obj: The connection where back pressure
            is being applied

        """
        self.logger.warning('Connection back pressure detected: %r', obj)

    def on_connection_blocked(self, method_frame):
        """This method is called by pika if RabbitMQ sends a connection blocked
        method, to let us know we need to throttle our publishing.

        :param pika.amqp_object.Method method_frame: The blocked method frame

        """
        self.logger.warning('Connection blocked: %s', method_frame)
        self.state = STATE_BLOCKED
        if self.on_unavailable:
            self.on_unavailable(self)

    def on_connection_unblocked(self, method_frame):
        """When RabbitMQ indicates the connection is unblocked, set the state
        appropriately.

        :param pika.amqp_object.Method method_frame: Unblocked method frame

        """
        self.logger.debug('Connection unblocked: %r', method_frame)
        self.state = STATE_READY
        if self.on_ready:
            self.on_ready(self)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.adapters.tornado_connection.TornadoConnection connection:
            Closed connection
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        start_state = self.state
        self.state = STATE_CLOSED
        if self.on_unavailable:
            self.on_unavailable(self)

        self.connection = None
        self.channel = None

        if start_state != STATE_CLOSING:
            self.logger.warning('%s closed while %s: (%s) %s',
                                connection, self.state_description,
                                reply_code, reply_text)
            self._reconnect()

    #
    # Error Condition Callbacks
    #

    def on_basic_return(self, _channel, method, properties, body):
        """Invoke a registered callback or log the returned message.

        :param pika.channel.Channel _channel: The channel the message was
            sent on
        :param pika.spec.Basic.Return method: The method object
        :param pika.spec.BasicProperties properties: The message properties
        :param body: The message body
        :type body: str, unicode, bytes

        """
        if self.on_return:
            self.on_return(method, properties, body)
        else:
            self.logger.critical(
                '%s message %s published to %s (CID %s) returned: %s',
                method.exchange, properties.message_id,
                method.routing_key, properties.correlation_id,
                method.reply_text)

    #
    # Channel event callbacks
    #

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        :param pika.channel.Channel channel: The channel object

        """
        self.logger.debug('Channel opened')
        self.channel = channel
        if self.publisher_confirmations:
            self.channel.confirm_delivery(self.on_delivery_confirmation)
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.add_on_flow_callback(self.on_channel_flow)
        self.channel.add_on_return_callback(self.on_basic_return)
        self.state = STATE_READY
        if self.on_ready:
            self.on_ready(self)

    def on_channel_closed(self, channel, reply_code, reply_text):
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
        for future in self.messages.values():
            future.set_exception(exceptions.AMQPException(reply_code,
                                                          reply_text))
        self.messages = {}
        if self.closing:
            self.logger.debug('Channel %s was intentionally closed (%s) %s',
                              channel, reply_code, reply_text)
        else:
            self.logger.warning('Channel %s was closed: (%s) %s',
                                channel, reply_code, reply_text)
            self.state = STATE_BLOCKED
            if self.on_unavailable:
                self.on_unavailable(self)
            self.channel = self._open_channel()

    def on_channel_flow(self, method):
        """When RabbitMQ indicates the connection is unblocked, set the state
        appropriately.

        :param pika.spec.Channel.Flow method: The Channel flow frame

        """
        if method.active:
            self.logger.info('Channel flow is active (READY)')
            self.state = STATE_READY
            if self.on_ready:
                self.on_ready(self)
        else:
            self.logger.warning('Channel flow is inactive (BLOCKED)')
            self.state = STATE_BLOCKED
            if self.on_unavailable:
                self.on_unavailable(self)
