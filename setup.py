#!/usr/bin/env python
import codecs
import sys

from setuptools import find_packages, setup

from sprockets.mixins import amqp

version = amqp.__version__
try:
    with open('LOCAL-VERSION') as f:
        local_version = f.readline().strip()
        version = version + local_version
except IOError:
    pass

setup(
    name='sprockets.mixins.amqp',
    description='Mixin for publishing events to RabbitMQ',
    version=version,
    packages=find_packages(),
    namespace_packages=['sprockets', 'sprockets.mixins'],
    test_suite='nose.collector',
    long_description=codecs.open('README.rst', encoding='utf-8').read(),
    install_requires=open('requires/installation.txt').read(),
    author='AWeber Communications, Inc.',
    author_email='api@aweber.com',
    license=codecs.open('LICENSE', encoding='utf-8').read(),
    url='https://github.com/sprockets/sprockets.mixins.amqp.git',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    namespace_packages=['sprockets'],
    py_modules=['sprockets.amqp'],
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    test_suite='nose.collector',
    zip_safe=True)
