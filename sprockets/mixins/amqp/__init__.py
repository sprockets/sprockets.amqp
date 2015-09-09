__import__('pkg_resources').declare_namespace(__name__)

version_info = (0, 0, 1)
__version__ = '.'.join(str(v) for v in version_info)

__all__ = ('version_info', '__version__', 'RabbitMQRequestMixin')
