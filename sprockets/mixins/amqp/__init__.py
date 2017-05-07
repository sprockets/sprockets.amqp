"""The PublishingMixin adds RabbitMQ publishing capabilities to a request
handler, with methods to speed the development of publishing RabbitMQ messages.

Configured using the following environment variables:

    ``AMQP_URL`` - The AMQP URL to connect to.
    ``AMQP_TIMEOUT`` - The optional maximum time to wait for a bad state
                       to resolve before treating the failure as
                       persistent.
    ``AMQP_RECONNECT_DELAY`` - The optional time in seconds to wait before
                               reconnecting on connection failure.
    ``AMQP_CONNECTION_ATTEMPTS`` - The optional number of connection
                                   attempts to make before giving up.

The ``AMQP``` prefix is interchangeable with ``RABBITMQ``. For example, you can
use ``AMQP_URL`` or ``RABBITMQ_URL``.

"""
import logging
import os
import sys

import sprockets_amqp.exceptions  # can be safely imported

try:
    from sprockets_amqp.client import Client
    from sprockets_amqp.web import PublishingMixin
    from tornado import concurrent, ioloop
    import pika.exceptions
except ImportError:  # pragma: nocover
    sys.stderr.write('setup.py import error compatibility objects created\n')
    concurrent, ioloop, pika, Client, PublishingMixin = \
        object(), object(), object(), object(), object()


LOGGER = logging.getLogger(__name__)


# Compatibility bindings -- please stop using these, use the ones from
# the appropriate sprockets_amqp module instead.
__version__ = sprockets_amqp.version
AMQPException = sprockets_amqp.exceptions.AMQPException
ConnectionStateError = sprockets_amqp.exceptions.ConnectionStateError
NotReadyError = sprockets_amqp.exceptions.NotReadyError
PublishingFailure = sprockets_amqp.exceptions.PublishingFailure

DEFAULT_RECONNECT_DELAY = 5
DEFAULT_CONNECTION_ATTEMPTS = 3


def install(application, io_loop=None, **kwargs):
    """Call this to install AMQP for the Tornado application. Additional
    keyword arguments are passed through to the constructor of the AMQP
    object.

    :param tornado.web.Application application: The tornado application
    :param tornado.ioloop.IOLoop io_loop: The current IOLoop.
    :rtype: bool

    """
    if getattr(application, 'amqp', None) is not None:
        LOGGER.warning('AMQP is already installed')
        return False

    kwargs.setdefault('io_loop', io_loop)

    # Support AMQP_* and RABBITMQ_* variables
    for prefix in {'AMQP', 'RABBITMQ'}:

        key = '{}_URL'.format(prefix)
        if os.environ.get(key) is not None:
            LOGGER.debug('Setting URL to %s', os.environ[key])
            kwargs.setdefault('url', os.environ[key])

        key = '{}_CONFIRMATIONS'.format(prefix)
        if os.environ.get(key) is not None:
            value = os.environ[key].lower() in {'true', '1'}
            LOGGER.debug('Setting enable_confirmations to %s', value)
            kwargs.setdefault('enable_confirmations', value)

        key = '{}_CONNECTION_ATTEMPTS'.format(prefix)
        if os.environ.get(key) is not None:
            value = int(os.environ[key])
            LOGGER.debug('Setting connection_attempts to %s', value)
            kwargs.setdefault('connection_attempts', value)

        key = '{}_RECONNECT_DELAY'.format(prefix)
        if os.environ.get(key) is not None:
            value = float(os.environ[key])
            LOGGER.debug('Setting reconnect_delay to %s', value)
            kwargs.setdefault('reconnect_delay', value)

    # Set the default AMQP app_id property
    if application.settings.get('service') and \
            application.settings.get('version'):
        default_app_id = '{}/{}'.format(
            application.settings['service'], application.settings['version'])
    else:
        default_app_id = 'sprockets.mixins.amqp/{}'.format(__version__)
    kwargs.setdefault('default_app_id', default_app_id)

    # Default the default URL value if not already set
    kwargs.setdefault('url', 'amqp://guest:guest@localhost:5672/%2f')

    LOGGER.debug('kwargs: %r', kwargs)
    setattr(application, 'amqp', Client(**kwargs))
    return True
