import logging
import os

import sprockets_amqp.client


def install(application, io_loop=None, **kwargs):
    """Call this to install AMQP for the Tornado application. Additional
    keyword arguments are passed through to the constructor of the AMQP
    object.

    :param tornado.web.Application application: The tornado application
    :param tornado.ioloop.IOLoop io_loop: The current IOLoop.
    :rtype: bool

    """
    logger = logging.getLogger('sprockets_amqp.web.install')
    if getattr(application, 'amqp', None) is not None:
        logger.warning('AMQP is already installed')
        return False

    kwargs.setdefault('io_loop', io_loop)

    # Support AMQP_* and RABBITMQ_* variables
    for prefix in {'AMQP', 'RABBITMQ'}:

        key = '{}_URL'.format(prefix)
        if os.environ.get(key) is not None:
            logger.debug('Setting URL to %s', os.environ[key])
            kwargs.setdefault('url', os.environ[key])

        key = '{}_CONFIRMATIONS'.format(prefix)
        if os.environ.get(key) is not None:
            value = os.environ[key].lower() in {'true', '1'}
            logger.debug('Setting enable_confirmations to %s', value)
            kwargs.setdefault('enable_confirmations', value)

        key = '{}_CONNECTION_ATTEMPTS'.format(prefix)
        if os.environ.get(key) is not None:
            value = int(os.environ[key])
            logger.debug('Setting connection_attempts to %s', value)
            kwargs.setdefault('connection_attempts', value)

        key = '{}_RECONNECT_DELAY'.format(prefix)
        if os.environ.get(key) is not None:
            value = float(os.environ[key])
            logger.debug('Setting reconnect_delay to %s', value)
            kwargs.setdefault('reconnect_delay', value)

    # Set the default AMQP app_id property
    if (application.settings.get('service') and
            application.settings.get('version')):
        default_app_id = '{}/{}'.format(
            application.settings['service'], application.settings['version'])
    else:
        default_app_id = 'sprockets.mixins.amqp/{}'.format(
            sprockets_amqp.version)
    kwargs.setdefault('default_app_id', default_app_id)

    # Default the default URL value if not already set
    kwargs.setdefault('url', 'amqp://guest:guest@localhost:5672/%2f')

    logger.debug('kwargs: %r', kwargs)
    setattr(application, 'amqp', sprockets_amqp.client.Client(**kwargs))

    return True


class PublishingMixin(object):
    """This mixin adds publishing messages to RabbitMQ. It uses a
    persistent connection and channel opened when the application
    start up and automatically reopened if closed by RabbitMQ

    """

    def amqp_publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ

        :param str exchange: The exchange to publish the message to
        :param str routing_key: The routing key to publish the message with
        :param bytes body: The message body to send
        :param dict properties: An optional dict of AMQP properties
        :rtype: tornado.concurrent.Future

        :raises: :exc:`sprockets_amqp.exceptions.AMQPException`
        :raises: :exc:`sprockets_amqp.exceptions.NotReadyError`
        :raises: :exc:`sprockets_amqp.exceptions.PublishingFailure`

        """
        properties = properties or {}
        if hasattr(self, 'correlation_id') and getattr(self, 'correlation_id'):
            properties.setdefault('correlation_id', self.correlation_id)
        return self.application.amqp.publish(
            exchange, routing_key, body, properties)
