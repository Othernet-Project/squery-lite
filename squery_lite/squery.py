"""
sqery.py: Helpers for working with databases

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import print_function

import os
import re
import logging
import sqlite3
import calendar
import datetime
import functools
import contextlib

from sqlize import (From, Where, Group, Order, Limit, Select, Update, Delete,
                    Insert, Replace, sqlin, sqlarray)
from pytz import utc

from .migrations import migrate
from .utils import basestring


SLASH = re.compile(r'\\')
SQLITE_DATE_TYPES = ('date', 'datetime', 'timestamp')
MAX_VARIABLE_NUMBER = 999


def from_utc_timestamp(timestamp):
    """Converts the passed-in unix UTC timestamp into a datetime object."""
    dt = datetime.datetime.utcfromtimestamp(float(timestamp))
    return dt.replace(tzinfo=utc)


def to_utc_timestamp(dt):
    """Converts the passed-in datetime object into a unix UTC timestamp."""
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        msg = "Naive datetime object passed. It is assumed that it's in UTC."
        logging.warning(msg)
    return calendar.timegm(dt.timetuple())


sqlite3.register_adapter(datetime.datetime, to_utc_timestamp)
for date_type in SQLITE_DATE_TYPES:
    sqlite3.register_converter(date_type, from_utc_timestamp)


def convert_query(fn):
    """ Ensure any SQLExpression instances are serialized

    :param qry:     raw SQL string or SQLExpression instance
    :returns:       raw SQL string
    """
    @functools.wraps(fn)
    def wrapper(self, qry, *args, **kwargs):
        if hasattr(qry, 'serialize'):
            qry = qry.serialize()
        assert isinstance(qry, basestring), 'Expected qry to be string'
        if self.debug:
            print('SQL:', qry)
        return fn(self, qry, *args, **kwargs)
    return wrapper


class Row(sqlite3.Row):
    """ sqlite.Row subclass that allows attribute access to items """
    def __getattr__(self, key):
        return self[key]

    def get(self, key, default=None):
        key = str(key)
        try:
            return self[key]
        except IndexError:
            return default

    def __contains__(self, key):
        return key in self.keys()


class Connection(object):
    """ Wrapper for sqlite3.Connection object """
    def __init__(self, path=':memory:',):
        self.path = path
        self.connect()

    def connect(self):
        self._conn = sqlite3.connect(self.path,
                                     detect_types=sqlite3.PARSE_DECLTYPES)
        self._conn.row_factory = Row

        # Allow manual transaction handling, see http://bit.ly/1C7E7EQ
        self._conn.isolation_level = None
        # More on WAL: https://www.sqlite.org/isolation.html
        # Requires SQLite >= 3.7.0
        cur = self._conn.cursor()
        cur.execute('PRAGMA journal_mode=WAL;')
        logging.debug('Connected to database {}'.format(self.path))

    def close(self):
        self._conn.commit()
        self._conn.close()

    def __getattr__(self, attr):
        conn = object.__getattribute__(self, '_conn')
        return getattr(conn, attr)

    def __setattr__(self, attr, value):
        if not hasattr(self, attr) or attr == '_conn':
            object.__setattr__(self, attr, value)
        else:
            setattr(self._conn, attr, value)

    def __repr__(self):
        return "<Connection path='%s'>" % self.path


class Cursor(object):
    def __init__(self, cursor, debug=False):
        self.cursor = cursor
        self.debug = debug

    @property
    def results(self):
        return self.cursor.fetchall()

    @property
    def result(self):
        return self.cursor.fetchone()

    def __iter__(self):
        return self.cursor

    @convert_query
    def query(self, qry, *params, **kwparams):
        """ Perform a query

        Any positional arguments are converted to a list of arguments for the
        query, and are used to populate any '?' placeholders. The keyword
        arguments are converted to a mapping which provides values to ':name'
        placeholders. These do not apply to SQLExpression instances.

        :param qry:     raw SQL or SQLExpression instance
        :returns:       cursor object
        """
        self.cursor.execute(qry, params or kwparams)

    @convert_query
    def execute(self, qry, *args, **kwargs):
        self.cursor.execute(qry, *args, **kwargs)

    @convert_query
    def executemany(self, qry, *args, **kwargs):
        self.cursor.executemany(qry, *args, **kwargs)

    def executescript(self, sql):
        self.cursor.executescript(sql)


class Database(object):

    migrate = staticmethod(migrate)
    # Provide access to query classes for easier access
    sqlin = staticmethod(sqlin)
    sqlarray = staticmethod(sqlarray)
    From = From
    Where = Where
    Group = Group
    Order = Order
    Limit = Limit
    Select = Select
    Update = Update
    Delete = Delete
    Insert = Insert
    Replace = Replace
    MAX_VARIABLE_NUMBER = MAX_VARIABLE_NUMBER

    def __init__(self, conn, debug=False):
        self.conn = conn
        self.debug = debug

    def cursor(self, debug=None):
        """
        Return a new cursor
        """
        debug = self.debug if debug is None else debug
        return Cursor(self.conn.cursor(), debug)

    def query(self, qry, *params, **kwparams):
        """ Perform a query

        Any positional arguments are converted to a list of arguments for the
        query, and are used to populate any '?' placeholders. The keyword
        arguments are converted to a mapping which provides values to ':name'
        placeholders. These do not apply to SQLExpression instances.

        :param qry:     raw SQL or SQLExpression instance
        :returns:       cursor object
        """
        cursor = self.cursor()
        cursor.query(qry, *params, **kwparams)
        return cursor

    def execute(self, qry, *args, **kwargs):
        cursor = self.cursor()
        cursor.execute(qry, *args, **kwargs)
        return cursor

    def executemany(self, qry, *args, **kwargs):
        cursor = self.cursor()
        cursor.executemany(qry, *args, **kwargs)
        return cursor

    def executescript(self, sql):
        cursor = self.cursor()
        cursor.executescript(sql)
        return cursor

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
        self.conn.commit()

    def refresh_table_stats(self):
        return self.execute('ANALYZE sqlite_master;')

    def acquire_lock(self):
        return self.execute('BEGIN EXCLUSIVE;')

    def close(self):
        self.conn.close()

    def reconnect(self):
        self.conn.connect()

    @property
    def connection(self):
        return self.conn

    @contextlib.contextmanager
    def transaction(self, silent=False):
        cursor = self.execute('BEGIN;')
        try:
            yield cursor
            self.commit()
        except Exception:
            self.rollback()
            if silent:
                return
            raise

    @classmethod
    def connect(cls, database, **kwargs):
        return Connection(database)

    def recreate(self, path):
        self.drop(path)
        self.connect(path)

    @classmethod
    def drop(cls, path):
        os.remove(path)

    def __repr__(self):
        return "<Database connection='%s'>" % self.conn.path


class DatabaseContainer(dict):

    def __init__(self, connections, debug=False):
        databases = dict((n, Database(c, debug=debug))
                         for n, c in connections.items())
        super(DatabaseContainer, self).__init__(databases)
        self.__dict__ = self
