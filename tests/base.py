import contextlib
import logging
import os
import uuid

from tornado import gen, locks, testing, web
import mock
from pika import frame, spec

from sprockets.mixins import amqp

LOGGER = logging.getLogger(__name__)


class RequestHandler(amqp.PublishingMixin, web.RequestHandler):

    def initialize(self):
        self.correlation_id = self.request.headers.get('Correlation-Id')

    @gen.coroutine
    def get(self, *args, **kwargs):
        LOGGER.debug('Handling Request %r', self.correlation_id)
        parameters = {
            'exchange': self.get_argument('exchange', str(uuid.uuid4())),
            'routing_key': self.get_argument('routing_key', str(uuid.uuid4())),
            'body': str(uuid.uuid4()),
            'properties': {
                'content_type': 'application/json',
                'message_id': str(uuid.uuid4()),
                'type': 'test-message'}}
        try:
            yield self.amqp_publish(**parameters)
        except amqp.AMQPException as error:
            self.write({'error': str(error),
                        'type': error.__class__.__name__,
                        'parameters': parameters})
        else:
            self.write(parameters)  # Correlation-ID is added pass by reference
            self.finish()
        LOGGER.debug('Responded')


class AsyncHTTPTestCase(testing.AsyncHTTPTestCase):

    AUTO_INSTALL = True

    def setUp(self):
        super(AsyncHTTPTestCase, self).setUp()
        self._environ = {}
        for prefix in {'AMQP', 'RABBITMQ'}:
            for suffix in {'URL',
                           'CONFIRMATIONS',
                           'CONNECTION_ATTEMPTS',
                           'RECONNECT_DELAY'}:
                key = '{}_{}'.format(prefix, suffix)
                if key in os.environ:
                    LOGGER.debug('Clearing %s', key)
                    self._environ[key] = os.environ[key]
                    del os.environ[key]
        self.correlation_id = str(uuid.uuid4())
        self.headers = {'Correlation-Id': self.correlation_id}
        self.connection = mock.Mock()
        self.channel = mock.Mock()
        self.channel.basic_publish = mock.Mock()
        self.connection.channel = mock.Mock(return_value=self.channel)
        self.connection.close = mock.Mock()
        self.on_ready = mock.Mock()
        self.on_unavailable = mock.Mock()
        if self.AUTO_INSTALL:
            self.assertTrue(self.install(**self.get_install_kwargs()))

    def tearDown(self):
        for key, value in self._environ.items():
            os.environ[key] = value

    def get_app(self):
        return web.Application([(r'/', RequestHandler)],
                               **{'service': 'test',
                                  'version': amqp.__version__})

    def get_install_kwargs(self):
        return {
            'on_ready_callback': self.on_ready,
            'on_unavailable_callback': self.on_unavailable
        }

    def install(self, **kwargs):
        with mock.patch('sprockets.mixins.amqp.Client.connect') as conn:
            conn.return_value = self.connection
            result = amqp.install(self._app, io_loop=self.io_loop, **kwargs)
            conn.assert_called_once()
            self.client = self._app.amqp
            self.client.connection = self.connection
            self.client.channel = self.channel
            self.client.state = amqp.Client.STATE_READY
        return result

    @contextlib.contextmanager
    def mock_publish(self, side_effect=None):
        if side_effect:
            self.channel.basic_publish.side_effect = side_effect
        yield self.channel.basic_publish

    def send_ack(self, *args):
        self.io_loop.add_callback(
            self.client.on_delivery_confirmation,
            frame.Method(1, spec.Basic.Ack(self.client.message_number, False)))

    def send_nack(self, *args):
        self.io_loop.add_callback(
            self.client.on_delivery_confirmation,
            frame.Method(1, spec.Basic.Nack(
                self.client.message_number, False)))
