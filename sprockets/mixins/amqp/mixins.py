import logging

from tornado import gen


LOGGER = logging.getLogger(__name__)


class PublishingMixin(object):
    """This mixin adds publishing messages to RabbitMQ. It uses a
    persistent connection and channel opened when the application
    start up and automatically reopened if closed by RabbitMQ

    """
    @property
    def app_id(self):
        """Return a value to be used for the app_id AMQP message property.

        :rtype: str

        """
        service = self.application.settings.get('service')
        version = self.application.settings.get('version')
        return '{}/{}'.format(service, version)

    @gen.coroutine
    def amqp_publish(self, exchange, routing_key, body, properties=None,
                     mandatory=False):
        """Publish the message to RabbitMQ

        Expects Correlation ID to be available via self.correlation_id,
        default value will be None if not set.
        The ``sprockets.mixins.correlation`` plugin provides this.

        :param str exchange: The exchange to publish the message to
        :param str routing_key: The routing key to publish the message with
        :param str,unicode,bytes body: The message body to send
        :param dict properties: An optional dict of additional properties
                                to append. Will not override mandatory
                                properties:
                                app_id, correlation_id, message_id, timestamp
        :param bool mandatory: Whether to instruct the server to return an
                               unqueueable message
                               http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish.mandatory

        """
        yield self.application.amqp.publish(
            exchange,
            routing_key,
            body,
            self.app_id,
            getattr(self, 'correlation_id', None),
            properties,
            mandatory
        )
