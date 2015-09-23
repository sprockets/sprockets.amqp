try:
    from .amqp import AMQPMixin
except ImportError:
    def AMQPMixin(*args):
        raise RuntimeError('failed to import sprockets.mixins.amqp')

version_info = (0, 0, 1)
__version__ = '.'.join(str(v) for v in version_info)

__all__ = ('version_info', '__version__', 'AMQPMixin')
