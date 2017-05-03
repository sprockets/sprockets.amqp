import json
import logging
import uuid

from pika import spec
from tornado import concurrent, locks, testing, web

from sprockets.mixins import amqp

from . import base

LOGGER = logging.getLogger(__name__)


def setUpModule():
    logging.getLogger('pika').setLevel(logging.INFO)


class AsyncHTTPTestCase(testing.AsyncHTTPTestCase):

    CONFIRMATIONS = True

    def setUp(self):
        super(AsyncHTTPTestCase, self).setUp()
        self.correlation_id = str(uuid.uuid4())
        self.exchange = str(uuid.uuid4())
        self.queue = str(uuid.uuid4())
        self.routing_key = str(uuid.uuid4())
        self.ready = locks.Event()
        amqp.install(self._app, self.io_loop, **{
            'on_ready_callback': self.on_amqp_ready,
            'enable_confirmations': self.CONFIRMATIONS,
            'url': 'amqp://guest:guest@127.0.0.1:5672/%2f'})
        self.io_loop.start()

    def get_app(self):
        return web.Application(
            [(r'/', base.RequestHandler)],
            **{'service': 'test', 'version': amqp.__version__})

    def get_message(self):
        future = concurrent.Future()

        def on_message(_channel, method, properties, body):
            future.set_result((method, properties, body))

        self._app.amqp.channel.basic_get(on_message, self.queue)
        return future

    def on_amqp_ready(self, _client):
        LOGGER.debug('AMQP ready')
        self._app.amqp.channel.exchange_declare(
            self.on_exchange_declared, self.exchange, auto_delete=True)

    def on_exchange_declared(self, frame):
        LOGGER.debug('Exchange declared: %r', frame)
        self._app.amqp.channel.queue_declare(
            self.on_queue_declared, self.queue, auto_delete=True)

    def on_queue_declared(self, frame):
        LOGGER.debug('Queue declared: %r', frame)
        self._app.amqp.channel.queue_bind(
            self.on_queue_bound, self.queue, self.exchange, self.routing_key)

    def on_queue_bound(self, frame):
        LOGGER.debug('Queue bound: %r', frame)
        self.io_loop.stop()


class PublisherTestCase(AsyncHTTPTestCase):

    CONFIRMATIONS = False

    @testing.gen_test
    def test_full_execution(self):
        response = yield self.http_client.fetch(
            self.get_url('/?exchange={}&routing_key={}'.format(
                self.exchange, self.routing_key)),
            headers={'Correlation-Id': self.correlation_id})
        published = json.loads(response.body.decode('utf-8'))
        fetched = yield self.get_message()
        self.assertIsInstance(fetched[0], spec.Basic.GetOk)
        self.assertEqual(fetched[1].correlation_id, self.correlation_id)
        self.assertEqual(fetched[2].decode('utf-8'), published['body'])


class PublisherConfirmationTestCase(AsyncHTTPTestCase):

    @testing.gen_test
    def test_full_execution(self):
        response = yield self.http_client.fetch(
            self.get_url('/?exchange={}&routing_key={}'.format(
                self.exchange, self.routing_key)),
            headers={'Correlation-Id': self.correlation_id})
        published = json.loads(response.body.decode('utf-8'))
        fetched = yield self.get_message()
        self.assertIsInstance(fetched[0], spec.Basic.GetOk)
        self.assertEqual(fetched[1].correlation_id, self.correlation_id)
        self.assertEqual(fetched[2].decode('utf-8'), published['body'])

    @testing.gen_test
    def test_publishing_exchange_failure(self):
        response = yield self.http_client.fetch(
            self.get_url('/?exchange=fail&routing_key=error'),
            headers={'Correlation-Id': self.correlation_id})
        result = json.loads(response.body.decode('utf-8'))
        self.assertEqual(
            result['error'],
            "AMQP Exception (404): NOT_FOUND - "
            "no exchange 'fail' in vhost '/'")
        self.assertEqual(result['type'], 'AMQPException')