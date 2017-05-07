class AMQPException(Exception):
    """Base Class for the the AMQP client"""
    fmt = 'AMQP Exception ({}): {}'

    def __init__(self, *args):
        super(AMQPException, self).__init__(*args)
        self._args = args

    def __str__(self):
        return self.fmt.format(*self._args)


class ConnectionStateError(AMQPException):
    """Invoked when reconnect is attempted but the state is incorrect"""
    fmt = 'Attempted to close the connection while {}'


class NotReadyError(AMQPException):
    """Raised if the :meth:`Client.publish` is invoked and the connection is
    not ready for publishing.

    """
    fmt = 'Connection is {} when publishing message {}'


class PublishingFailure(AMQPException):
    """Raised if the :meth:`Client.publish` is invoked and an error occurs or
    the message delivery is not confirmed.

    """
    fmt = 'Message {} was not routed to its intended destination ({}, {}): {}'
