import json
import os
import pika
import time
import unittest
import uuid

from tornado import gen, testing, web, ioloop, httpclient

from sprockets.mixins.amqp import amqp


ROUTING_KEY = 'test_mixins_amqp'
EXCHANGE = 'amqp_mixin'
os.environ.setdefault('AMQP', 'amqp://guest:guest@localhost/%2F')
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}


class TestConsumer(object):

    """A consumer worker used to verify a message was published."""

    def __init__(self, routing_key, exchange):
        """Create an exchange and a queue to consume messages.

        Bind to a queue and assign a callback function, ``process_event`` to
        that queue.

        """
        self.queue_name = 'test_{0}'.format(str(uuid.uuid4()))
        url = os.environ['AMQP']
        self.connection = pika.BlockingConnection(pika.URLParameters(url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=self.queue_name, auto_delete=True, exclusive=True)
        self.channel.exchange_declare(
            exchange=exchange,
            auto_delete=False,
            durable=True,
            type='topic',
        )
        self.channel.queue_bind(
            queue=self.queue_name,
            exchange=exchange,
            routing_key=routing_key,
        )

    def publish_message(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key='subscriber.queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(content_type="application/json",
                                            delivery_mode=1))

    def get_message(self):
        while True:
            method, _, body = self.channel.basic_get(queue=self.queue_name)
            if method is None:
                time.sleep(1)
            else:
                self.channel.basic_ack(method.delivery_tag)
                return body


class AsyncTestHandler(amqp.RabbitMQRequestMixin,
                       web.RequestHandler):

    def initialize(self):
        super(AsyncTestHandler, self).initialize()
        self.prepare_called = False

    @gen.coroutine
    def prepare(self):
        maybe_future = super(AsyncTestHandler, self).prepare()
        if maybe_future:
            yield maybe_future
        self.prepare_called = True

    @gen.coroutine
    def get(self, message):
        self._publish_message(exchange=EXCHANGE,
                              routing_key=ROUTING_KEY,
                              message=message,
                              properties=None)
        self.write(json.dumps({'message': message}))
        self.finish()


class _BaseTestPublishing(testing.AsyncHTTPTestCase):

    def setUp(self):
        super(_BaseTestPublishing, self).setUp()
        self.consumer = TestConsumer(ROUTING_KEY, EXCHANGE)

    def get_new_ioloop(self):
        return ioloop.IOLoop.instance()

    def get_app(self):
        return web.Application([web.url(r'/(?P<message>[\-\w]{32,36})',
                               AsyncTestHandler)])

    def trigger_worker_to_publish(self):
        resp = self.fetch('/{0}'.format(self.msg_id), headers=HEADERS)
        self.body = json.loads(resp.body.decode('utf-8'))

    def test_establish_rabbit_connection(self):
        self.assertEqual(self.received, self.msg_id)

    def test_rabbit_conn_connected(self):
        self.assertIsNotNone(amqp.CONNECTION)

    def test_rabbit_channel_connected(self):
        self.assertIsNotNone(amqp.CHANNEL)


class AsyncAMQPTest(_BaseTestPublishing):

    msg_id = str(uuid.uuid4())

    def setUp(self):
        super(AsyncAMQPTest, self).setUp()
        self.trigger_worker_to_publish()
        self.received = self.consumer.get_message().decode('utf-8')


class AsyncAMQPTestLostConnection(_BaseTestPublishing):

    msg_id = str(uuid.uuid4())

    def setUp(self):
        super(AsyncAMQPTestLostConnection, self).setUp()
        self.trigger_worker_to_publish()
        self.received = self.consumer.get_message().decode('utf-8')

        amqp.CONNECTION = None
        amqp.CHANNEL = None
        self.trigger_worker_to_publish()
        self.received = self.consumer.get_message().decode('utf-8')
