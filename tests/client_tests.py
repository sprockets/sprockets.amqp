import logging

import mock
from pika import exceptions, frame, spec

from sprockets.mixins import amqp

from . import base

LOGGER = logging.getLogger(__name__)


class ClientStateTestCase(base.AsyncHTTPTestCase):

    STATES = {'idle', 'connecting', 'ready', 'blocked', 'closing', 'closed'}

    def assert_other_states_false(self, current):
        self.assertTrue(all([getattr(self.client, state) is False
                             for state in self.STATES if state != current]))

    def test_idle(self):
        self.client.state = amqp.Client.STATE_IDLE
        self.assertTrue(self.client.idle)
        self.assertEqual(self.client.state_description, 'Idle')
        self.assert_other_states_false('idle')

    def test_connecting(self):
        self.client.state = amqp.Client.STATE_CONNECTING
        self.assertTrue(self.client.connecting)
        self.assertEqual(self.client.state_description, 'Connecting')
        self.assert_other_states_false('connecting')

    def test_ready(self):
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        self.assertEqual(self.client.state_description, 'Ready')
        self.assert_other_states_false('ready')

    def test_blocked(self):
        self.client.state = amqp.Client.STATE_BLOCKED
        self.assertTrue(self.client.blocked)
        self.assertEqual(self.client.state_description, 'Blocked')
        self.assert_other_states_false('blocked')

    def test_closing(self):
        self.client.state = amqp.Client.STATE_CLOSING
        self.assertTrue(self.client.closing)
        self.assertEqual(self.client.state_description, 'Closing')
        self.assert_other_states_false('closing')

    def test_closed(self):
        self.client.state = amqp.Client.STATE_CLOSED
        self.assertTrue(self.client.closed)
        self.assertEqual(self.client.state_description, 'Closed')
        self.assert_other_states_false('closed')

    def test_closable_true(self):
        for state in {amqp.Client.STATE_READY, amqp.Client.STATE_BLOCKED}:
            self.client.state = state
            self.assertTrue(self.client.closable)

    def test_closable_false(self):
        for state in {amqp.Client.STATE_IDLE,
                      amqp.Client.STATE_CONNECTING,
                      amqp.Client.STATE_CLOSING,
                      amqp.Client.STATE_CLOSED}:
            self.client.state = state
            self.assertFalse(self.client.closable)


class ClientStateTransitionsTestCase(base.AsyncHTTPTestCase):

    def test_connection_blocked(self):
        self.assertEqual(self.on_unavailable.call_count, 0)
        blocked_frame = frame.Method(
            0, spec.Connection.Blocked('This is a reason'))
        self.client.on_connection_blocked(blocked_frame)
        self.assertEqual(self.on_unavailable.call_count, 1)
        self.assertTrue(self.client.blocked)

    def test_connection_blocked_no_unavailable(self):
        self.client.on_unavailable = None
        blocked_frame = frame.Method(
            0, spec.Connection.Blocked('This is a reason'))
        self.client.on_connection_blocked(blocked_frame)
        self.assertTrue(self.client.blocked)

    def test_connection_unblocked(self):
        self.client.state = amqp.Client.STATE_IDLE
        self.assertTrue(self.client.idle)
        unblocked_frame = frame.Method(0, spec.Connection.Unblocked())
        self.client.on_connection_unblocked(unblocked_frame)
        self.assertEqual(self.on_ready.call_count, 1)
        self.assertTrue(self.client.ready)

    def test_connection_unblocked_no_unavailable(self):
        self.client.state = amqp.Client.STATE_IDLE
        self.assertTrue(self.client.idle)
        self.client.on_ready = None
        unblocked_frame = frame.Method(0, spec.Connection.Unblocked())
        self.client.on_connection_unblocked(unblocked_frame)
        self.assertTrue(self.client.ready)

    def test_close(self):
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        self.client.close()
        self.assertEqual(self.client.connection.close.call_count, 1)
        self.assertTrue(self.client.closing)

    def test_close_when_in_wrong_state(self):
        self.client.state = amqp.Client.STATE_CLOSING
        self.assertTrue(self.client.closing)
        with self.assertRaises(amqp.ConnectionStateError):
            self.client.close()

    def test_on_connection_close_when_closing(self):
        self.assertEqual(self.on_unavailable.call_count, 0)
        self.client.state = amqp.Client.STATE_CLOSING
        self.assertTrue(self.client.closing)
        self.client.on_connection_closed(
            self.client.connection, 200, 'Normal Shutdown')
        self.assertTrue(self.client.closed)
        self.assertIsNone(self.client.connection)
        self.assertIsNone(self.client.channel)
        self.assertEqual(self.on_unavailable.call_count, 1)

    def test_on_connection_close_no_unavailable_callback(self):
        self.client.state = amqp.Client.STATE_CLOSING
        self.assertTrue(self.client.closing)
        self.client.on_unavailable = None
        self.client.on_connection_closed(
            self.client.connection, 200, 'Normal Shutdown')
        self.assertTrue(self.client.closed)
        self.assertIsNone(self.client.connection)
        self.assertIsNone(self.client.channel)

    def test_on_connection_close_when_ready(self):
        self.assertEqual(self.on_unavailable.call_count, 0)
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        with mock.patch('sprockets.mixins.amqp.Client._reconnect') as reconnect:
            self.client.on_connection_closed(
                self.client.connection, 400, 'You Done Goofed')
            reconnect.assert_called_once()
        self.assertTrue(self.client.closed)
        self.assertIsNone(self.client.connection)
        self.assertIsNone(self.client.channel)
        self.assertEqual(self.on_unavailable.call_count, 1)

    def test_channel_flow_enabled(self):
        self.client.state = amqp.Client.STATE_BLOCKED
        self.assertTrue(self.client.blocked)
        self.client.on_channel_flow(spec.Channel.Flow(True))
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        self.assertEqual(self.on_ready.call_count, 1)

    def test_channel_flow_enabled_no_callback(self):
        self.channel.on_ready = None
        self.client.state = amqp.Client.STATE_BLOCKED
        self.assertTrue(self.client.blocked)
        self.client.on_channel_flow(spec.Channel.Flow(True))
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)

    def test_channel_flow_disabled(self):
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        self.client.on_channel_flow(spec.Channel.Flow(False))
        self.client.state = amqp.Client.STATE_BLOCKED
        self.assertTrue(self.client.blocked)
        self.assertEqual(self.on_unavailable.call_count, 1)

    def test_channel_flow_disabled_no_callback(self):
        self.client.on_unavailable = None
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        self.client.on_channel_flow(spec.Channel.Flow(False))
        self.client.state = amqp.Client.STATE_BLOCKED
        self.assertTrue(self.client.blocked)

    def test_on_channel_close(self):
        self.client.state = amqp.Client.STATE_READY
        self.assertTrue(self.client.ready)
        with mock.patch(
                'sprockets.mixins.amqp.Client._open_channel') as open_channel:
            self.client.on_channel_closed(1, 400, 'You Done Goofed')
            self.assertTrue(self.client.blocked)
            self.assertEqual(self.on_unavailable.call_count, 1)
            open_channel.assert_called_once()

    def test_on_channel_close_when_closing(self):
        self.client.state = amqp.Client.STATE_CLOSING
        self.assertTrue(self.client.closing)
        self.client.on_channel_closed(1, 200, 'Normal Shutdown')

    def test_on_channel_open(self):
        self.client.state = amqp.Client.STATE_CONNECTING
        self.assertTrue(self.client.connecting)
        self.client.on_channel_open(self.client.channel)
        self.assertTrue(self.client.ready)
        self.assertEqual(self.on_ready.call_count, 1)

    def test_on_channel_open_no_callback(self):
        self.client.on_ready = None
        self.client.state = amqp.Client.STATE_CONNECTING
        self.assertTrue(self.client.connecting)
        self.client.on_channel_open(self.client.channel)
        self.assertTrue(self.client.ready)

    def test_connect_raises_when_in_wrong_state(self):
        self.client.state = amqp.Client.STATE_CONNECTING
        self.assertTrue(self.client.connecting)
        with self.assertRaises(amqp.ConnectionStateError):
            self.client.connect()

    def test_reconnect(self):
        self.client.state = amqp.Client.STATE_IDLE
        self.assertTrue(self.client.idle)
        with mock.patch.object(
                self.client.io_loop, 'call_later') as call_later:
            self.client._reconnect()
            call_later.assert_called_once_with(self.client.reconnect_delay,
                                               self.client.connect)

    def test_reconnect_when_closing(self):
        self.client.state = amqp.Client.STATE_CLOSING
        self.assertTrue(self.client.closing)
        with mock.patch.object(
                self.client.io_loop, 'call_later') as call_later:
            self.client._reconnect()
            call_later.assert_not_called()

    def test_on_connection_open_error(self):
        self.client.state = amqp.Client.STATE_CONNECTING
        self.assertTrue(self.client.connecting)
        with mock.patch.object(self.client, '_reconnect') as reconnect:
            self.client.on_connection_open_error(
                self.connection, exceptions.AMQPConnectionError('200', 'Error'))
            reconnect.assert_called_once()
        self.assertTrue(self.client.closed)
