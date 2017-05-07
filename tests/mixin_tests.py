import json

import pika.exceptions

from sprockets_amqp import client, exceptions

from . import base


class ConfirmationsDisabledMixinTestCase(base.AsyncHTTPTestCase):

    def get_install_kwargs(self):
        return {'enable_confirmations': False}

    def test_mixin_invokes_amqp_publish(self):
        with self.mock_publish() as publish:
            response = self.fetch('/', headers=self.headers)
            params = json.loads(response.body.decode('utf-8'))
            self.assertEqual(publish.call_args[0][0], params['exchange'])
            self.assertEqual(publish.call_args[0][1], params['routing_key'])
            self.assertEqual(publish.call_args[0][2], params['body'])
            for key, expectation in params['properties'].items():
                self.assertEqual(getattr(publish.call_args[0][3], key),
                                 expectation)

    def test_not_ready_raised(self):
        self.client.state = client.STATE_BLOCKED
        with self.mock_publish():
            response = self.fetch('/', headers=self.headers)
            result = json.loads(response.body.decode('utf-8'))
            error_expectation = exceptions.NotReadyError.fmt.format(
                self.client.state_description,
                result['parameters']['properties']['message_id'])
            self.assertEqual(result['error'], error_expectation)
            self.assertEqual(result['type'], 'NotReadyError')


class ConfirmationsEnabledMixinTestCase(base.AsyncHTTPTestCase):

    def test_mixin_invokes_amqp_publish(self):
        with self.mock_publish(side_effect=self.send_ack) as publish:
            response = self.fetch('/', headers=self.headers)
            params = json.loads(response.body.decode('utf-8'))
            self.assertEqual(publish.call_args[0][0], params['exchange'])
            self.assertEqual(publish.call_args[0][1], params['routing_key'])
            self.assertEqual(publish.call_args[0][2], params['body'])
            for key, expectation in params['properties'].items():
                self.assertEqual(getattr(publish.call_args[0][3], key),
                                 expectation)

    def test_mixin_amqp_publish_without_correlation_id(self):
        with self.mock_publish(side_effect=self.send_ack) as publish:
            response = self.fetch('/')
            params = json.loads(response.body.decode('utf-8'))
            self.assertEqual(publish.call_args[0][0], params['exchange'])
            self.assertEqual(publish.call_args[0][1], params['routing_key'])
            self.assertEqual(publish.call_args[0][2], params['body'])
            for key, expectation in params['properties'].items():
                self.assertEqual(getattr(publish.call_args[0][3], key),
                                 expectation)
            self.assertIsNone(publish.call_args[0][3].correlation_id)

    def test_not_ready_raised(self):
        self.client.state = client.STATE_BLOCKED
        with self.mock_publish():
            response = self.fetch('/', headers=self.headers)
            result = json.loads(response.body.decode('utf-8'))
            error_expectation = exceptions.NotReadyError.fmt.format(
                self.client.state_description,
                result['parameters']['properties']['message_id'])
            self.assertEqual(result['error'], error_expectation)
            self.assertEqual(result['type'], 'NotReadyError')

    def test_publishing_failure_raised(self):
        with self.mock_publish(side_effect=pika.exceptions.AMQPChannelError()):
            response = self.fetch('/', headers=self.headers)
            result = json.loads(response.body.decode('utf-8'))
            error_expectation = exceptions.PublishingFailure.fmt.format(
                result['parameters']['properties']['message_id'],
                result['parameters']['exchange'],
                result['parameters']['routing_key'],
                pika.exceptions.AMQPChannelError.__name__)
            self.assertEqual(result['error'], error_expectation)
            self.assertEqual(result['type'], 'PublishingFailure')
