#!/usr/bin/env python

import os
from setuptools import setup


VERSION = '2.0.dev4'


def read(fname):
    """ Return content of specified file """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='squery-lite',
    version=VERSION,
    author='Outernet Inc',
    author_email='apps@outernet.is',
    url='https://github.com/Outernet-Project/squery-lite',
    license='BSD',
    packages=['squery_lite'],
    include_package_data=True,
    long_description=read('README.rst'),
    install_requires=[
        'pytz',
        'sqlize',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
    ],
)
