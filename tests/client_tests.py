import logging

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
