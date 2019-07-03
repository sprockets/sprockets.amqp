Version History
===============

`2.1.5`_ July 3, 2019
---------------------
- Remove official support for python versions less than 3.5
- Add support for tornado 5.X releases

`2.1.4`_ Jan 24, 2019
---------------------
- Pin pika to 0.12.0, 0.11.0 has issues with Python 3.7

`2.1.3`_ Jan 23, 2019
---------------------
- Pin pika to 0.11.0

`2.1.2`_ May 3, 2017
--------------------
- Move setting default properties up one level to allow access when not ready

`2.1.1`_ May 3, 2017
--------------------
- Fix overlogging

`2.1.0`_ May 3, 2017
--------------------
- Fix intentional closing of an AMQP connection
- New behavior for publishing that raises exception
- Add publisher confirmations
- Make ``sprockets.mixins.amqp.install()`` work with `sprockets.http <https://github.com/sprockets/sprockets.http>`_
- Add support for environment variables prefixed with ``AMQP_`` or ``RABBITMQ``
- Clean up AMQP message property behavior, make defaults, but don't change already set values
- Automatically create the default ``app_id`` AMQP message property
- Split out tests into a mix of unit tests and integration tests
- Update state behaviors, names, and transitions
- All publishing is mandatory, returned messages are logged, a callback can be registered

`2.0.0`_ Apr 24, 2017
---------------------
- Move Mixin and AMQP client to separate files
- Replace AMQP connection handling code with latest internal version
- Provide ability to register callbacks for ready, unavailable, and persistent failure states
- Remove default AMQP URL from AMQP class, url is now a required parameter for install
- Rename amqp_publish 'message' parameter to 'body'
- Add properties for all AMQP states
- Provide mandatory AMQP properties (app_id, correlation_id, message_id, timestamp) automatically
    - Mandatory properties cannot be overridden
- Add unit test coverage for new functionality
    - Test execution requires a running AMQP server

`1.0.1`_ Feb 28, 2016
---------------------
- Fixed documentation links and generation.

`1.0.0`_ Mar 15, 2016
---------------------
- Connect to AMQP in ``sprockets.mixins.amqp.install`` and maintain and persist connection
- Change to use tornado locks.Condition vs locks.Event

`0.1.4`_ Mar 09, 2016
---------------------
- Reconnect in connection close callback

`0.1.3`_ Sept 28, 2015
----------------------
- Use packages instead of py_modules

`0.1.2`_ Sept 25, 2015
----------------------
- Don't log the message body

`0.1.1`_ Sept 24, 2015
----------------------
- Clean up installation and testing environment

`0.1.0`_ Sept 23, 2015
----------------------
 - Initial implementation

.. _Next Release: https://github.com/sprockets/sprockets.amqp/compare/2.1.5...HEAD
.. _2.1.5: https://github.com/sprockets/sprockets.amqp/compare/2.1.4...2.1.5
.. _2.1.4: https://github.com/sprockets/sprockets.amqp/compare/2.1.3...2.1.4
.. _2.1.3: https://github.com/sprockets/sprockets.amqp/compare/2.1.2...2.1.3
.. _2.1.2: https://github.com/sprockets/sprockets.amqp/compare/2.1.1...2.1.2
.. _2.1.1: https://github.com/sprockets/sprockets.amqp/compare/2.1.0...2.1.1
.. _2.1.0: https://github.com/sprockets/sprockets.amqp/compare/2.0.0...2.1.0
.. _2.0.0: https://github.com/sprockets/sprockets.amqp/compare/1.0.1...2.0.0
.. _1.0.1: https://github.com/sprockets/sprockets.amqp/compare/1.0.0...1.0.1
.. _1.0.0: https://github.com/sprockets/sprockets.amqp/compare/0.1.4...1.0.0
.. _0.1.4: https://github.com/sprockets/sprockets.amqp/compare/0.1.3...0.1.4
.. _0.1.3: https://github.com/sprockets/sprockets.amqp/compare/0.1.2...0.1.3
.. _0.1.2: https://github.com/sprockets/sprockets.amqp/compare/0.1.1...0.1.2
.. _0.1.1: https://github.com/sprockets/sprockets.amqp/compare/0.1.0...0.1.1
.. _0.1.0: https://github.com/sprockets/sprockets.amqp/compare/551982c...0.1.0
