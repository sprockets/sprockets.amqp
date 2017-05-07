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
