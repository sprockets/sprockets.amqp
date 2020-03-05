import json
import logging
import os
import sys
import uuid

from pika import spec
from tornado import concurrent, locks, testing, web

from sprockets.mixins import amqp
from tests import base

LOGGER = logging.getLogger(__name__)


def setUpModule():
    try:
        with open('build/test-environment') as f:
            for line in f:
                if line.startswith('export '):
                    line = line[7:]
                name, _, value = line.strip().partition('=')
                os.environ[name] = value
    except IOError:
        pass
    logging.getLogger('pika').setLevel(logging.ERROR)
    logging.getLogger('sprockets.mixins.amqp').setLevel(logging.ERROR)


class AsyncHTTPTestCase(testing.AsyncHTTPTestCase):

    CONFIRMATIONS = True

    def setUp(self):
        super().setUp()
        self.correlation_id = str(uuid.uuid4())
        self.exchange = str(uuid.uuid4())
        self.get_delivered_message = concurrent.Future()
        self.get_returned_message = concurrent.Future()
        self.queue = str(uuid.uuid4())
        self.routing_key = str(uuid.uuid4())
        self.ready = locks.Event()
        amqp.install(self._app, self.io_loop, **{
            'on_ready_callback': self.on_amqp_ready,
            'enable_confirmations': self.CONFIRMATIONS,
            'on_return_callback': self.on_message_returned,
            'url': os.environ['AMQP_URL']})

        def wait_on_ready():
            if self._app.amqp.ready:
                self.io_loop.stop()
            else:
                self.io_loop.call_later(0.1, wait_on_ready)

        sys.stdout.flush()
        self.io_loop.add_callback(wait_on_ready)
        self.io_loop.start()

    def tearDown(self):
        def shutdown():
            if self._app.amqp.closed:
                self.io_loop.stop()
            elif not self._app.amqp.closing:
                self._app.amqp.close()
            self.io_loop.call_later(0.1, shutdown)

        self.io_loop.add_callback(shutdown)
        self.io_loop.start()
        super().tearDown()

    def get_app(self):
        return web.Application(
            [(r'/', base.RequestHandler)],
            **{'service': 'test', 'version': amqp.__version__})

    def on_amqp_ready(self, _client):
        LOGGER.debug('AMQP ready')
        self._app.amqp.channel.exchange_declare(
            self.on_exchange_declared, self.exchange,
            durable=False, auto_delete=True)

    def on_exchange_declared(self, frame):
        LOGGER.debug('Exchange declared: %r', frame)
        self._app.amqp.channel.queue_declare(
            self.on_queue_declared, self.queue,
            arguments={'x-expires': 30000},
            auto_delete=True, durable=False)

    def on_queue_declared(self, frame):
        LOGGER.debug('Queue declared: %r', frame)
        self._app.amqp.channel.queue_bind(
            self.on_queue_bound, self.queue, self.exchange, self.routing_key)

    def on_queue_bound(self, frame):
        LOGGER.debug('Queue bound: %r', frame)
        self._app.amqp.channel.basic_consume(
            self.on_message_delivered, self.queue)
        self.io_loop.stop()

    def on_message_delivered(self, _channel, method, properties, body):
        self.get_delivered_message.set_result((method, properties, body))

    def on_message_returned(self, method, properties, body):
        self.get_returned_message.set_result((method, properties, body))


class PublisherTestCase(AsyncHTTPTestCase):

    CONFIRMATIONS = False

    @testing.gen_test
    async def test_full_execution(self):
        response = await self.http_client.fetch(
            self.get_url('/?exchange={}&routing_key={}'.format(
                self.exchange, self.routing_key)),
            headers={'Correlation-Id': self.correlation_id})
        published = json.loads(response.body.decode('utf-8'))
        delivered = await self.get_delivered_message
        self.assertIsInstance(delivered[0], spec.Basic.Deliver)
        self.assertEqual(delivered[1].correlation_id, self.correlation_id)
        self.assertEqual(delivered[2].decode('utf-8'), published['body'])


class PublisherConfirmationTestCase(AsyncHTTPTestCase):

    @testing.gen_test
    async def test_full_execution(self):
        response = await self.http_client.fetch(
            self.get_url('/?exchange={}&routing_key={}'.format(
                self.exchange, self.routing_key)),
            headers={'Correlation-Id': self.correlation_id})
        published = json.loads(response.body.decode('utf-8'))
        delivered = await self.get_delivered_message
        self.assertIsInstance(delivered[0], spec.Basic.Deliver)
        self.assertEqual(delivered[1].correlation_id, self.correlation_id)
        self.assertEqual(delivered[2].decode('utf-8'), published['body'])

    @testing.gen_test
    async def test_publishing_exchange_failure(self):
        response = await self.http_client.fetch(
            self.get_url('/?exchange=fail&routing_key=error'),
            headers={'Correlation-Id': self.correlation_id})
        result = json.loads(response.body.decode('utf-8'))
        self.assertEqual(
            result['error'],
            'AMQP Exception (404): NOT_FOUND - '
            "no exchange 'fail' in vhost '/'")
        self.assertEqual(result['type'], 'AMQPException')

    @testing.gen_test
    async def test_published_message_returned(self):
        response = await self.http_client.fetch(
            self.get_url('/?exchange={}&routing_key=error'.format(
                 self.exchange)),
            headers={'Correlation-Id': self.correlation_id})
        published = json.loads(response.body.decode('utf-8'))
        returned = await self.get_returned_message
        self.assertEqual(returned[0].exchange, self.exchange)
        self.assertEqual(returned[0].reply_code, 312)
        self.assertEqual(returned[0].reply_text, 'NO_ROUTE')
        self.assertEqual(returned[0].routing_key, 'error')
        self.assertEqual(returned[1].correlation_id, self.correlation_id)
        self.assertEqual(returned[2].decode('utf-8'), published['body'])
