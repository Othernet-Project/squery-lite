import os
from setuptools import setup, find_packages

VERSION = '1.1'


def read(fname):
    """ Return content of specified file """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='squery-lite',
    version=VERSION,
    license='BSD',
    packages=[pkg.__name__],
    include_package_data=True,
    long_description=read('README.rst'),
    install_requires=[
        'pytz',
        'sqlize',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Software Development :: Libraries',
        'Environment :: Web Environment',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
