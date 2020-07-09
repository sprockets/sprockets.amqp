#!/usr/bin/env python
import os.path

import setuptools

from sprockets.mixins.amqp import __version__


def read_requirements(name):
    requirements = []
    try:
        with open(os.path.join('requires', name)) as req_file:
            for line in req_file:
                if '#' in line:
                    line = line[:line.index('#')]
                line = line.strip()
                if line.startswith('-r'):
                    requirements.extend(read_requirements(line[2:].strip()))
                elif line and not line.startswith('-'):
                    requirements.append(line)
    except IOError:
        pass
    return requirements


setuptools.setup(
    name='sprockets.mixins.amqp',
    version=__version__,
    description='Mixin for publishing events to RabbitMQ',
    long_description=open('README.rst').read(),
    url='https://github.com/sprockets/sprockets.mixins.amqp',
    author='AWeber Communications, Inc.',
    author_email='api@aweber.com',
    license='BSD',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    packages=setuptools.find_packages(exclude=['tests']),
    namespace_packages=['sprockets', 'sprockets.mixins'],
    install_requires=read_requirements('installation.txt'),
    zip_safe=True)
