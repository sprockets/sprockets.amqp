sprockets.mixins.amqp
=====================
AMQP Publishing Mixin for Tornado RequestHandlers.

|Version| |Downloads| |Travis| |CodeCov| |Docs|

Installation
------------
``sprockets.mixins.amqp`` is available on the
`Python Package Index <https://pypi.python.org/pypi/sprockets.mixins.amqp>`_
and can be installed via ``pip`` or ``easy_install``:

.. code-block:: bash

   pip install sprockets.mixins.amqp

Documentation
-------------
https://pythonhosted.org/sprockets.mixins.amqp

Requirements
------------
- pika>=0.10.0
- tornado>=4.2.0

Example
-------

You may optionally install ``sprockets.mixins.correlation`` into your request handler to take advantage of automatic correlation_id fetching.
Otherwise, be sure to set correlation_id as an instance variable on your request handler before sending AMQP messages.

This examples demonstrates the most basic usage of ``sprockets.mixins.amqp``

.. code:: bash

   export AMQP_URL="amqp://user:password@rabbitmq_host:5672/%2f"
   python my-example-app.py


.. code:: python

   import json

   from tornado import gen, web
   from sprockets.mixins import amqp

   def make_app(**settings):
       application = web.Application(
           [
               web.url(r'/', RequestHandler),
           ], **settings)

       amqp_settings = {
           "reconnect_delay": 5,
       }

       amqp.install(application, **amqp_settings)
       return application


   class RequestHandler(amqp.PublishingMixin,
                        correlation.HandlerMixin,
                        web.RequestHandler):

       @gen.coroutine
       def get(self, *args, **kwargs):
           body = {'request': self.request.path, 'args': args, 'kwargs': kwargs}
           yield self.amqp_publish(
               'exchange',
               'routing.key',
               json.dumps(body),
               {'content_type': 'application/json'}
           )

AMQP Settings
^^^^^^^^^^^^^
:url: The AMQP URL to connect to.
:reconnect_delay: The optional time in seconds to wait before reconnecting on connection failure.
:timeout: The optional maximum time to wait for a bad state to resolve before treating the failure as persistent.
:connection_attempts: The optional number of connection attempts to make before giving up.
:on_ready_callback: The optional callback to call when the connection to the AMQP server has been established and is ready.
:on_unavailable_callback: The optional callback to call when the connection to the AMQP server becomes unavailable.
:on_persistent_failure_callback: The optional callback to call when the connection failure does not resolve itself within the timeout.
:on_message_returned_callback: The optional callback to call when the AMQP server returns a message.
:ioloop: An optional IOLoop to override the default with.

Environment Variables
^^^^^^^^^^^^^^^^^^^^^
Any environment variables set will override the corresponding AMQP settings passed into install()

- AMQP_URL
- AMQP_TIMEOUT
- AMQP_RECONNECT_DELAY
- AMQP_CONNECTION_ATTEMPTS

Source
------
``sprockets.mixins.amqp`` source is available on Github at `https://github.com/sprockets/sprockets.mixins.amqp <https://github.com/sprockets/sprockets.mixins.amqp>`_

License
-------
``sprockets.mixins.amqp`` is released under the `3-Clause BSD license <https://github.com/sprockets/sprockets.mixins.amqp/blob/master/LICENSE>`_.

.. |Version| image:: https://badge.fury.io/py/sprockets.mixins.amqp.svg?
   :target: http://badge.fury.io/py/sprockets.mixins.amqp

.. |Travis| image:: https://travis-ci.org/sprockets/sprockets.mixins.amqp.svg?branch=master
   :target: https://travis-ci.org/sprockets/sprockets.mixins.amqp

.. |CodeCov| image:: http://codecov.io/github/sprockets/sprockets.mixins.amqp/coverage.svg?branch=master
   :target: https://codecov.io/github/sprockets/sprockets.mixins.amqp?branch=master

.. |Downloads| image:: https://pypip.in/d/sprockets.mixins.amqp/badge.svg?
   :target: https://pypi.python.org/pypi/sprockets.mixins.amqp

.. |Docs| image:: https://img.shields.io/badge/docs-pythonhosted-green.svg
   :target: https://pythonhosted.com/sprockets.mixins.amqp
