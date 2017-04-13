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
"""
import os
import logging

LOGGER = logging.getLogger(__name__)

try:
    from .mixins import PublishingMixin
    from .amqp import AMQP

except ImportError as error:
    class PublishingMixin(object):
        def __init__(self, *args, **kwargs):
            raise error

    class AMQP(object):
        def __init__(self, *args, **kwargs):
            raise error

version_info = (2, 0, 0)
__version__ = '.'.join(str(v) for v in version_info)


def install(application, **kwargs):
    """Call this to install AMQP for the Tornado application.

    :rtype: bool

    """
    if getattr(application, 'amqp', None) is not None:
        LOGGER.warning('AMQP is already installed')
        return False

    if os.environ.get('AMQP_URL'):
        kwargs['url'] = os.environ['AMQP_URL']
    if os.environ.get('AMQP_TIMEOUT'):
        kwargs['timeout'] = int(os.environ['AMQP_TIMEOUT'])
    if os.environ.get('AMQP_RECONNECT_DELAY'):
        kwargs['reconnect_delay'] = int(os.environ['AMQP_RECONNECT_DELAY'])
    if os.environ.get('AMQP_CONNECTION_ATTEMPTS'):
        kwargs['connection_attempts'] = int(
            os.environ['AMQP_CONNECTION_ATTEMPTS'])

    setattr(application, 'amqp', AMQP(**kwargs))
    return True
