import logging
import os

from tornado import web

from sprockets.mixins import amqp

from . import base

LOGGER = logging.getLogger(__name__)


class DoubleInstallTestCase(base.AsyncHTTPTestCase):

    def test_double_install(self):
        self.assertFalse(amqp.install(self._app))


class InstallDefaultsTestCase(base.AsyncHTTPTestCase):

    def get_app(self):
        return web.Application()

    def test_default_app_id(self):
        self.assertEqual(self.client.default_app_id,
                         'sprockets.mixins.amqp/{}'.format(amqp.__version__))

    def test_default_publisher_confirmations(self):
        self.assertTrue(self.client.publisher_confirmations)

    def test_default_connection_attempts(self):
        self.assertEqual(self.client.parameters.connection_attempts,
                         amqp.DEFAULT_CONNECTION_ATTEMPTS)

    def test_default_reconnect_delay(self):
        self.assertEqual(self.client.reconnect_delay,
                         amqp.DEFAULT_RECONNECT_DELAY)


class InstallKWArgsTestCase(base.AsyncHTTPTestCase):

    url = 'amqp://test:user@test-host:5672/test'

    def get_install_kwargs(self):
        return {'url': self.url}

    def test_installed_url_value(self):
        self.assertEqual(self.client.url, self.url)

    def test_default_app_id(self):
        self.assertEqual(self.client.default_app_id,
                         'test/{}'.format(amqp.__version__))

    def test_io_loop(self):
        self.assertEqual(self.client.io_loop, self.io_loop)


class InstallEnvironTestCase(base.AsyncHTTPTestCase):

    AUTO_INSTALL = False

    def test_url_with_amqp_prefix(self):
        expectation = 'amqp://test:user@test-host:5672/test'
        os.environ['AMQP_URL'] = expectation
        self.install()
        self.assertEqual(self._app.amqp.url, expectation)

    def test_url_with_rabbitmq_prefix(self):
        expectation = 'amqp://test:user@test-host:5672/test'
        os.environ['RABBITMQ_URL'] = expectation
        self.install()
        self.assertEqual(self._app.amqp.url, expectation)

    def test_connection_parameters(self):
        os.environ['RABBITMQ_URL'] = 'amqps://test:user@test-host:5671/test'
        self.install()
        self.assertEqual(self._app.amqp.parameters.ssl, True)
        self.assertEqual(self._app.amqp.parameters.host, 'test-host')
        self.assertEqual(self._app.amqp.parameters.port, 5671)
        self.assertEqual(self._app.amqp.parameters.virtual_host, 'test')
        self.assertEqual(self._app.amqp.parameters.credentials.username, 'test')
        self.assertEqual(self._app.amqp.parameters.credentials.password, 'user')

    def test_confirmations_true_amqp_prefix(self):
        os.environ['AMQP_CONFIRMATIONS'] = 'True'
        self.install()
        self.assertTrue(self._app.amqp.publisher_confirmations)

    def test_confirmations_false_amqp_prefix(self):
        os.environ['AMQP_CONFIRMATIONS'] = 'fAlSE'
        self.install()
        self.assertFalse(self._app.amqp.publisher_confirmations)

    def test_confirmations_true_rabbitmq_prefix(self):
        os.environ['RABBITMQ_CONFIRMATIONS'] = 'true'
        self.install()
        self.assertTrue(self._app.amqp.publisher_confirmations)

    def test_confirmations_1_rabbitmq_prefix(self):
        os.environ['RABBITMQ_CONFIRMATIONS'] = '1'
        self.install()
        self.assertTrue(self._app.amqp.publisher_confirmations)

    def test_confirmations_false_rabbitmq_prefix(self):
        os.environ['RABBITMQ_CONFIRMATIONS'] = 'FALSE'
        self.install()
        self.assertFalse(self._app.amqp.publisher_confirmations)

    def test_connection_attempts_with_amqp_prefix(self):
        os.environ['AMQP_CONNECTION_ATTEMPTS'] = '5'
        self.install()
        self.assertEqual(self._app.amqp.parameters.connection_attempts, 5)

    def test_connection_attempts_with_rabbitmq_prefix(self):
        os.environ['RABBITMQ_CONNECTION_ATTEMPTS'] = '42'
        self.install()
        self.assertEqual(self._app.amqp.parameters.connection_attempts, 42)

    def test_reconnect_delay_with_amqp_prefix(self):
        os.environ['AMQP_RECONNECT_DELAY'] = '2.3'
        self.install()
        self.assertEqual(self._app.amqp.reconnect_delay, 2.3)

    def test_reconnect_delay_with_rabbitmq_prefix(self):
        os.environ['RABBITMQ_RECONNECT_DELAY'] = '10.5'
        self.install()
        self.assertEqual(self._app.amqp.reconnect_delay, 10.5)


class InstallValueErrorTestCase(base.AsyncHTTPTestCase):

    AUTO_INSTALL = False

    def test_connection_attempts_when_str(self):
        with self.assertRaises(ValueError):
            self.install(connection_attempts='foo')

    def test_connection_attempts_when_zero(self):
        with self.assertRaises(ValueError):
            self.install(connection_attempts=0)

    def test_reconnect_delay_when_str(self):
        with self.assertRaises(ValueError):
            self.install(reconnect_delay='foo')

    def test_reconnect_delay_when_zero(self):
        with self.assertRaises(ValueError):
            self.install(reconnect_delay=0)

