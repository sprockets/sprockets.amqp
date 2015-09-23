import logging
import uuid

from pika import spec

from tornado import gen
from tornado import locks
from tornado import testing

from sprockets.mixins import amqp

LOGGER = logging.getLogger(__name__)


class BaseTestCase(testing.AsyncTestCase):

    @gen.coroutine
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.amqp = amqp.AMQP()
        self.exchange = str(uuid.uuid4())
        self.queue = str(uuid.uuid4())
        self.routing_key = str(uuid.uuid4())
        self.ready = locks.Event()
        self.get_response = locks.Event()
        self.message = None

        # This will finish when the AMQP connection is complete
        yield self.amqp.maybe_connect()

        LOGGER.info('Connected to RabbitMQ, declaring exchange %s',
                    self.exchange)
        self.amqp.channel.exchange_declare(self.on_exchange_declare_ok,
                                           self.exchange,
                                           auto_delete=True)

    def on_exchange_declare_ok(self, _method):
        LOGGER.info('Exchange %s declared, declaring queue %s',
                    self.exchange, self.queue)
        self.amqp.channel.queue_declare(self.on_queue_declare_ok,
                                        queue=self.queue)

    def on_queue_declare_ok(self, _method):
        LOGGER.info('Queue %s declared', self.queue)
        self.amqp.channel.queue_bind(self.on_bind_ok,
                                     self.queue,
                                     self.exchange,
                                     self.routing_key)

    def on_bind_ok(self, _method):
        LOGGER.info('Queue %s bound to %s', self.queue, self.exchange)
        self.amqp.channel.add_callback(self.on_get_response,
                                       [spec.Basic.GetEmpty],
                                       False)
        self.ready.set()

    def on_get_response(self, channel, method, properties=None, body=None):
        LOGGER.info('get_response: %r', method)
        self.message = method, properties, body
        self.get_response.set()

    @gen.coroutine
    def get_message(self):
        self.message = None
        self.get_response.clear()
        self.amqp.channel.basic_get(self.on_get_response, self.queue)

        LOGGER.info('Waiting on get')
        yield self.get_response.wait()
        if isinstance(self.message[0], spec.Basic.GetEmpty):
            raise ValueError('Basic.GetEmpty')
        raise gen.Return(self.message)


class AMQPIntegrationTests(BaseTestCase):

    @testing.gen_test
    def publishing_tests(self):
        yield self.ready.wait()
        LOGGER.info('Should be ready')
        message = uuid.uuid4().hex.encode('latin-1')
        yield self.amqp.publish(self.exchange, self.routing_key, message,
                                {'content_type': 'text/plain'})
        LOGGER.info('Published')
        result = yield self.get_message()
        self.assertEqual(message, result[2])
