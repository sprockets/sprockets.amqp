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
import sys

import sprockets_amqp.exceptions  # can be safely imported

try:
    from sprockets_amqp.client import Client
    from sprockets_amqp.web import install, PublishingMixin
    from tornado import concurrent, ioloop
    import pika.exceptions
except ImportError:  # pragma: nocover
    sys.stderr.write('setup.py import error compatibility objects created\n')
    concurrent, install, ioloop, pika, Client, PublishingMixin = \
        object(), object(), object(), object(), object(), object()


# Compatibility bindings -- please stop using these, use the ones from
# the appropriate sprockets_amqp module instead.
__version__ = sprockets_amqp.version
AMQPException = sprockets_amqp.exceptions.AMQPException
ConnectionStateError = sprockets_amqp.exceptions.ConnectionStateError
NotReadyError = sprockets_amqp.exceptions.NotReadyError
PublishingFailure = sprockets_amqp.exceptions.PublishingFailure

DEFAULT_RECONNECT_DELAY = 5
DEFAULT_CONNECTION_ATTEMPTS = 3
