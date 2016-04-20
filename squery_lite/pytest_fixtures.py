"""
This module contains fixtures for use with py.test test runner.
"""

import pytest

from .testing import TestContainer


@pytest.fixture(scope='session')
def database_container(request, database_config):
    """
    A test database container with automatic tear-down. This fixture requires
    you to define a ``database_config`` session-scoped fixture that returns a
    dict containing keyword arguments for the :py:class:`TestContainer` class.

    To speed the tests up, no set-up is done.

    This is a session-scoped fixture and tear-down is performed after all tests
    are done.
    """
    container = TestContainer(**database_config)

    def teardown():
        container.teardownall()
    request.addfinalizer(teardown)
    return container


@pytest.fixture
def databases(database_container):
    """
    Database container with setup performed for all databases. You should
    create your own fixtures that depend on the :py:func:`database_container`
    fixture if you wish to avoid running the setup on all tables.
    """
    database_container.setupall()
    return database_container
