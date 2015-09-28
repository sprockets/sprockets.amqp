import setuptools

setuptools.setup(
    name='sprockets.mixins.amqp',
    version='0.1.3',
    description='Mixin for publishing events to RabbitMQ',
    long_description=open('README.rst').read(),
    url='https://github.com/sprockets/sprockets.mixins.amqp',
    author='AWeber Communications, Inc.',
    author_email='api@aweber.com',
    license='BSD',
    classifiers=[
        'Development Status :: 3 - Alpha', 'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License', 'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    packages=setuptools.find_packages(),
    namespace_packages=['sprockets', 'sprockets.mixins'],
    install_requires=open('requires/installation.txt').read(),
    zip_safe=True)
