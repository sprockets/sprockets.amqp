import logging
import os
import unittest

import sprockets.amqp

LOGGER = logging.getLogger(__name__)
os.environ['ENVIRONMENT'] = 'testing'

class Prototype(object):
    pass
