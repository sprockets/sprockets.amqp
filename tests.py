import logging
import os
import multiprocessing
import uuid

from pika import spec
from tornado import gen, locks, testing, web
import mock

from sprockets.mixins import amqp

PIKA_BLOCKED_PATH = 'pika.spec.Connection.Blocked'
PIKA_UNBLOCKED_PATH = 'pika.spec.Connection.Unblocked'
PIKA_FLOW_PATH = 'pika.spec.Channel.Flow'
PIKA_CHANNEL_PATH = 'pika.channel.Channel'
PIKA_METHOD_PATH = 'pika.spec.Basic.Return'
PIKA_PROPERTIES_PATH = 'pika.spec.BasicProperties'

# Set this URL to that of a running AMQP server before executing tests
if 'TEST_AMQP_URL' in os.environ:
    AMQP_URL = os.environ['TEST_AMQP_URL']
else:
    AMQP_URL = 'amqp://guest:guest@localhost:5672/%2f'

LOGGER = logging.getLogger(__name__)


class TestRequestHandler(amqp.PublishingMixin):
    def __init__(self, application):
        self.application = application
        self.correlation_id = str(uuid.uuid4())


class BaseTestCase(testing.AsyncTestCase):

    @gen.coroutine
    def setUp(self):
        super(BaseTestCase, self).setUp()

        # make sure that our logging statements get executed
        amqp.amqp.LOGGER.enabled = True
        amqp.amqp.LOGGER.setLevel(logging.DEBUG)
        amqp.mixins.LOGGER.enabled = True
        amqp.mixins.LOGGER.setLevel(logging.DEBUG)

        self.exchange = str(uuid.uuid4())
        self.queue = str(uuid.uuid4())
        self.routing_key = str(uuid.uuid4())
        self.correlation_id = str(uuid.uuid4())
        self.message = None
        self.test_queue_bound = locks.Event()
        self.get_response = locks.Event()
        self.amqp_ready = locks.Event()
        self.condition = locks.Condition()
        self.config = {
            "url": AMQP_URL,
            "reconnect_delay": 1,
            "timeout": 2,
            "on_ready_callback": self.on_ready,
            "on_unavailable_callback": self.on_unavailable,
            "on_persistent_failure_callback": self.on_persistent_failure,
            "on_message_returned_callback": self.on_message_returned,
            "io_loop": self.io_loop,
        }
        self.app = web.Application()
        self.app.settings = {
            'service': 'unit_tests',
            'version': '0.0'
        }
        self.handler = TestRequestHandler(self.app)

        self.clear_event_tracking()

        amqp.install(self.app, **self.config)
        yield self.condition.wait(self.io_loop.time() + 5)

        LOGGER.info('Connected to RabbitMQ, declaring exchange %s',
                    self.exchange)
        self.app.amqp.channel.exchange_declare(self.on_exchange_declare_ok,
                                               self.exchange,
                                               auto_delete=True)

    def on_exchange_declare_ok(self, _method):
        LOGGER.info(
            'Exchange %s declared, declaring queue %s',
            self.exchange,
            self.queue
        )
        self.app.amqp.channel.queue_declare(self.on_queue_declare_ok,
                                            queue=self.queue,
                                            auto_delete=True)

    def on_queue_declare_ok(self, _method):
        LOGGER.info('Queue %s declared', self.queue)
        self.app.amqp.channel.queue_bind(self.on_bind_ok, self.queue,
                                         self.exchange, self.routing_key)

    def on_bind_ok(self, _method):
        LOGGER.info('Queue %s bound to %s', self.queue, self.exchange)
        self.app.amqp.channel.add_callback(self.on_get_response,
                                           [spec.Basic.GetEmpty], False)
        self.test_queue_bound.set()

    def on_get_response(self, channel, method, properties=None, body=None):
        LOGGER.info('get_response: %r', method)
        self.message = {
            'method': method,
            'properties': properties,
            'body': body,
        }
        self.get_response.set()

    def on_ready(self, caller):
        LOGGER.info('on_ready called')
        self.ready_called = True
        self.amqp_ready.set()

    def on_unavailable(self, caller):
        LOGGER.info('on_unavailable called')
        self.unavailable_called = True
        self.amqp_ready.clear()

    def on_persistent_failure(self, caller, exchange, routing_key,
                              body, properties, mandatory):
        LOGGER.info('on_persistent_failure called')
        self.persistent_failure_called = True
        self.failed_message = {
            'exchange': exchange,
            'routing_key': routing_key,
            'body': body,
            'properties': properties,
        }
        self.amqp_ready.clear()

    def on_message_returned(self, caller, method, properties, body):
        LOGGER.info('on_message_returned called')
        self.message_returned_called = True
        self.message_returned_error = method.reply_text
        self.returned_message = {
            'exchange': method.exchange,
            'routing_key': method.routing_key,
            'body': body,
            'properties': properties,
        }

    def clear_event_tracking(self):
        self.ready_called = False
        self.unavailable_called = False
        self.persistent_failure_called = False
        self.message_returned_called = False
        self.message_returned_error = None
        self.failed_message = {
            'exchange': None,
            'routing_key': None,
            'body': None,
            'properties': None,
        }
        self.returned_message = {
            'exchange': None,
            'routing_key': None,
            'body': None,
            'properties': None,
        }

    @gen.coroutine
    def get_message(self):
        self.message = None
        self.get_response.clear()
        self.app.amqp.channel.basic_get(self.on_get_response, self.queue)

        LOGGER.info('Waiting on get')
        yield self.get_response.wait()
        if isinstance(self.message['method'], spec.Basic.GetEmpty):
            raise ValueError('Basic.GetEmpty')
        raise gen.Return(self.message)


class InstallTests(testing.AsyncTestCase):

    @testing.gen_test(timeout=10)
    def should_override_settings_with_env_var_settings_test(self):
        amqp_env_vars = {
            'AMQP_URL': 'amqp://guest:guest@127.0.0.1:5672/%2f',
            'AMQP_TIMEOUT': '15',
            'AMQP_RECONNECT_DELAY': '7',
            'AMQP_CONNECTION_ATTEMPTS': '2',
        }
        app = web.Application()
        with mock.patch.dict('os.environ', amqp_env_vars):
            amqp.install(app,
                         url=AMQP_URL,
                         timeout=8,
                         reconnect_delay=5,
                         connection_attempts=3)

            self.assertEqual(app.amqp._url,
                             amqp_env_vars['AMQP_URL'])
            self.assertEqual(app.amqp._timeout,
                             int(amqp_env_vars['AMQP_TIMEOUT']))
            self.assertEqual(app.amqp._reconnect_delay,
                             int(amqp_env_vars['AMQP_RECONNECT_DELAY']))
            self.assertEqual(app.amqp._connection_attempts,
                             int(amqp_env_vars['AMQP_CONNECTION_ATTEMPTS']))

    @testing.gen_test(timeout=10)
    def should_raise_attribute_error_when_timeout_lt_reconnect_delay_test(self):
        with self.assertRaises(AttributeError) as context:
            amqp.install(web.Application(),
                         url=AMQP_URL,
                         timeout=4,
                         reconnect_delay=5)

            self.assertTrue(
                'Reconnection must be less than timeout' == str(
                    context.exception))

    @testing.gen_test(timeout=10)
    def should_raise_attribute_error_when_timeout_eq_reconnect_delay_test(self):
        with self.assertRaises(AttributeError) as context:
            amqp.install(web.Application(),
                         url=AMQP_URL,
                         timeout=5,
                         reconnect_delay=5)

            self.assertTrue(
                'Reconnection must be less than timeout' == str(
                    context.exception))


class EventsTests(BaseTestCase):

    @testing.gen_test(timeout=10)
    def should_call_on_ready_test(self):
        yield self.amqp_ready.wait()
        self.assertTrue(self.ready_called)

    @testing.gen_test(timeout=10)
    def should_call_on_unavailable_when_connection_closed_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Closing AMQP connection')
        self.app.amqp._connection.close()
        yield self.amqp_ready.wait()
        self.assertTrue(self.unavailable_called)

    @testing.gen_test(timeout=10)
    def should_not_call_on_unavailable_when_closing_closed_connection_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Mocking Closing state')
        self.app.amqp._state = self.app.amqp.STATE_CLOSING
        LOGGER.info('Closing AMQP connection')
        self.app.amqp._connection.close()
        self.assertFalse(self.unavailable_called)

    @testing.gen_test(timeout=10)
    def should_call_on_unavailable_when_connection_blocked_test(self):
        yield self.amqp_ready.wait()
        self.clear_event_tracking()
        LOGGER.info('Calling _on_connection_blocked')
        with mock.patch(PIKA_BLOCKED_PATH) as blocked:
            blocked.reason = 'Testing'
            self.app.amqp._on_connection_blocked(blocked)
            self.assertTrue(self.unavailable_called)

    @testing.gen_test(timeout=10)
    def should_call_on_ready_when_connection_unblocked_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Calling _on_connection_unblocked')
        with mock.patch(PIKA_UNBLOCKED_PATH) as unblocked:
            self.app.amqp._on_connection_unblocked(unblocked)
        yield self.amqp_ready.wait()
        self.assertTrue(self.ready_called)

    @testing.gen_test(timeout=10)
    def should_call_on_unavailable_when_channel_closed_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Closing AMQP channel')
        self.app.amqp.channel.close()
        yield self.amqp_ready.wait()
        self.assertTrue(self.unavailable_called)

    @testing.gen_test(timeout=10)
    def should_call_on_unavailable_when_channel_open_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Calling _on_channel_open')
        with mock.patch(PIKA_CHANNEL_PATH) as channel:
            self.app.amqp._on_channel_open(channel)
        yield self.amqp_ready.wait()
        self.assertTrue(self.ready_called)

    @testing.gen_test(timeout=10)
    def should_call_on_unavailable_when_channel_flow_inactive_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Calling _on_channel_flow (active=False)')
        with mock.patch(PIKA_FLOW_PATH) as flow:
            flow.active = False
            self.app.amqp._on_channel_flow(flow)
            self.assertTrue(self.unavailable_called)

    @testing.gen_test(timeout=10)
    def should_call_on_ready_when_channel_flow_active_test(self):
        yield self.amqp_ready.wait()
        self.amqp_ready.clear()
        self.clear_event_tracking()
        LOGGER.info('Calling _on_channel_flow (active=True)')
        with mock.patch(PIKA_FLOW_PATH) as flow:
            flow.active = True
            self.app.amqp._on_channel_flow(flow)
        yield self.amqp_ready.wait()
        self.assertTrue(self.ready_called)

    @testing.gen_test(timeout=10)
    def should_call_on_message_returned_test(self):
        yield self.amqp_ready.wait()
        self.clear_event_tracking()
        LOGGER.info('Calling _on_message_returned')
        with mock.patch(PIKA_CHANNEL_PATH) as channel, \
                mock.patch(PIKA_METHOD_PATH) as method, \
                mock.patch(PIKA_PROPERTIES_PATH) as properties:
            method.exchange = self.exchange
            method.routing_key = self.routing_key
            method.reply_text = 'TESTING'
            properties.correlation_id = self.correlation_id
            properties.message_id = str(uuid.uuid4())
            self.app.amqp._on_message_returned(channel, method, properties, "")
            self.assertTrue(self.message_returned_called)
            self.assertEqual(self.exchange, self.returned_message['exchange'])
            self.assertEqual(self.routing_key,
                             self.returned_message['routing_key'])
            self.assertEqual("", self.returned_message['body'])
            self.assertEqual(properties, self.returned_message['properties'])
            self.assertEqual('TESTING', self.message_returned_error)

    @testing.gen_test(timeout=10)
    def should_call_on_persistent_failure_when_state_set_connecting_test(self):
        yield self.test_queue_bound.wait()
        yield self.amqp_ready.wait()
        LOGGER.info('Mocking Connecting state')
        self.app.amqp._state = self.app.amqp.STATE_CONNECTING
        self._send_message_expecting_persistent_failure()

    @testing.gen_test(timeout=10)
    def should_call_on_persistent_failure_when_state_set_closing_test(self):
        yield self.test_queue_bound.wait()
        yield self.amqp_ready.wait()
        LOGGER.info('Mocking Closing state')
        self.app.amqp._state = self.app.amqp.STATE_CLOSING
        self._send_message_expecting_persistent_failure()

    @testing.gen_test(timeout=10)
    def should_call_on_persistent_failure_when_state_set_closed_test(self):
        yield self.test_queue_bound.wait()
        yield self.amqp_ready.wait()
        LOGGER.info('Mocking Closed state')
        self.app.amqp._state = self.app.amqp.STATE_CLOSED
        self._send_message_expecting_persistent_failure()

    @testing.gen_test(timeout=10)
    def should_call_on_persistent_failure_when_connection_closed_test(self):
        yield self.test_queue_bound.wait()
        yield self.amqp_ready.wait()
        LOGGER.info('Closing AMQP connection')
        self.app.amqp.close()
        self._send_message_expecting_persistent_failure()

    @testing.gen_test(timeout=10)
    def should_call_on_persistent_failure_when_connection_blocked_test(self):
        yield self.test_queue_bound.wait()
        yield self.amqp_ready.wait()
        LOGGER.info('Calling _on_connection_blocked')
        with mock.patch(PIKA_BLOCKED_PATH) as blocked:
            blocked.reason = 'Testing'
            self.app.amqp._on_connection_blocked(blocked)
            self._send_message_expecting_persistent_failure()

    def _send_message_expecting_persistent_failure(self):
        self.clear_event_tracking()
        message = bytes(bytearray(range(255, 0, -1)))
        properties = {'content_type': 'application/octet-stream'}
        yield self.app.amqp.publish(self.exchange, self.routing_key, message,
                                    self.handler.app_id, self.correlation_id,
                                    properties)
        self.assertTrue(self.persistent_failure_called)
        self.assertEqual(self.exchange, self.failed_message['exchange'])
        self.assertEqual(self.routing_key, self.failed_message['routing_key'])
        self.assertEqual(message, self.failed_message['body'])
        self.assertEqual(properties, self.failed_message['properties'])


class AMQPIntegrationTests(BaseTestCase):

    @testing.gen_test(timeout=10)
    def should_publish_message_test(self):
        yield self.test_queue_bound.wait()

        LOGGER.info('Should be ready')

        properties = {'content_type': 'application/octet-stream'}

        message = bytes(bytearray(range(255, 0, -1)))
        yield self.app.amqp.publish(
            self.exchange,
            self.routing_key,
            message,
            self.handler.app_id,
            self.correlation_id,
            properties
        )

        LOGGER.info('Published')

        result = yield self.get_message()
        self.assertEqual(message, result['body'])
        self.assertEqual(properties['content_type'],
                         result['properties'].content_type)

    @testing.gen_test(timeout=10)
    def should_clobber_mandatory_properties_test(self):
        yield self.test_queue_bound.wait()

        LOGGER.info('Should be ready')

        my_app_id = str(uuid.uuid4())
        my_correlation_id = str(uuid.uuid4())
        my_message_id = str(uuid.uuid4())

        properties = {
            'content_type': 'application/octet-stream',
            'app_id': my_app_id,
            'correlation_id': my_correlation_id,
            'message_id': my_message_id,
            'timestamp': 0,
        }

        message = bytes(bytearray(range(255, 0, -1)))
        yield self.app.amqp.publish(
            self.exchange,
            self.routing_key,
            message,
            self.handler.app_id,
            self.correlation_id,
            properties
        )

        LOGGER.info('Published')

        result = yield self.get_message()
        self.assertNotEqual(my_app_id, result['properties'].app_id)
        self.assertNotEqual(
            my_correlation_id, result['properties'].correlation_id)
        self.assertNotEqual(my_message_id, result['properties'].message_id)
        self.assertNotEqual(0, result['properties'].timestamp)

    @testing.gen_test(timeout=10)
    def should_preserve_additional_properties_test(self):
        yield self.test_queue_bound.wait()

        LOGGER.info('Should be ready')

        properties = {
            'content_type': 'application/octet-stream',
            'priority': 252,
        }

        message = bytes(bytearray(range(255, 0, -1)))
        yield self.app.amqp.publish(
            self.exchange,
            self.routing_key,
            message,
            self.handler.app_id,
            self.correlation_id,
            properties
        )

        LOGGER.info('Published')

        result = yield self.get_message()
        self.assertEqual('application/octet-stream',
                         result['properties'].content_type)
        self.assertEqual(252, result['properties'].priority)

    @testing.gen_test(timeout=10)
    def should_set_default_app_id_test(self):
        yield self.test_queue_bound.wait()

        LOGGER.info('Should be ready')

        default_app_id = '{}/{}'.format(multiprocessing.current_process().name,
                                        os.getpid())

        properties = {'content_type': 'application/octet-stream'}

        message = bytes(bytearray(range(255, 0, -1)))
        yield self.app.amqp.publish(
            self.exchange,
            self.routing_key,
            message,
            None,
            self.correlation_id,
            properties
        )

        LOGGER.info('Published')

        result = yield self.get_message()
        self.assertEqual(default_app_id, result['properties'].app_id)

    @testing.gen_test(timeout=10)
    def should_publish_message_via_handler_test(self):
        yield self.test_queue_bound.wait()

        LOGGER.info('Should be ready')

        message = bytes(bytearray(range(255, 0, -1)))
        properties = {'content_type': 'application/octet-stream'}

        yield self.handler.amqp_publish(
            self.exchange,
            self.routing_key,
            message,
            properties
        )

        LOGGER.info('Published')

        result = yield self.get_message()

        self.assertEqual(message, result['body'])
        self.assertEqual(self.handler.app_id, result['properties'].app_id)
        self.assertEqual(self.handler.correlation_id,
                         result['properties'].correlation_id)
        self.assertEqual(properties['content_type'],
                         result['properties'].content_type)
