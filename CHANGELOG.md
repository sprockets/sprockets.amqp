# Release History

## 2.0.0 (2017-04-20)
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

## 0.1.0 (2015-09-08)
- Initial release
