"""
This module contains classes and functions useful when testing an application
using squery-pg.
"""

import os
import hashlib

from .squery import Database


def random_name(prefix='test'):
    """
    Return random name that can be used to create the test database.
    """
    rndbytes = os.urandom(8)
    md5 = hashlib.md5()
    md5.update(rndbytes)
    return '{}_{}'.format(prefix, md5.hexdigest()[:7])


class TestContainer(object):
    """
    Return an instance of a database container that adds features relevant to
    testing. The container supports mutliple databases.

    The ``databases`` argument should be an iterable containing database
    information. Each member should be a dict that has 'name' and 'migrations'
    keys. The 'name' key maps to the database name, while the 'migration' key
    should be a string representing the name of the Python package that
    contains migrations for the database. Migrations are optional.

    .. note::
        The actual name of the database will be the database name with a
        '_test_<random_string>' suffix, while we still refer to it using the
        name specified in the ``databases`` argument.

    ``conf`` argument is a dictionary of application options that are passed to
    the migrations.

    The test runner / test code should invoke the
    :py:meth:`~TestContainer.setup` method to create an migrate the database,
    and :py:meth:`~TestContainer.teardown` method to tear it down and
    disconnect. Note that it is not necessary to tear down before set-up as the
    set-up code will recreate all tables. Full tear-down is only necessary
    after all test code that requires the database has finished using it.

    The location for the databases is specified using the ``dbdir`` argument.
    The default value is ``'/tmp'``
    """

    def __init__(self, databases, dbdir='/tmp', conf={}):
        self.dbdir = dbdir
        self.conf = conf
        self.databases = {}
        self.add_databases(databases)

    def add_databases(self, databases):
        for database in databases:
            self.add_database(database)

    def add_database(self, database):
        name = database['name']
        migrations = database.get('migrations')
        db_file = os.path.join(
            self.dbdir, random_name('test_{}'.format(name)))
        conn = Database.connect(database=db_file)
        self.databases[name] = {
            'file': db_file,
            'db': Database(conn),
            'migrations': migrations,
        }

    def load_fixtures(self, dbname, table, data):
        """
        Load fixtures from ``data`` iterable into specified table. The data is
        epxeced to be an iterable of dicts.
        """
        db = self.databases[dbname]['db']
        db.execute('BEGIN')
        for row in data:
            columns = row.keys()
            q = db.Insert(table, cols=columns)
            db.execute(q, row)
        db.execute('COMMIT')

    def setupall(self):
        for dbname in self.databases:
            self.setup(dbname)

    def setup(self, dbname):
        db = self.databases[dbname]['db']
        path = self.databases[dbname]['file']
        migrations = self.databases[dbname]['migrations']
        db.recreate(path)
        if migrations:
            Database.migrate(db, migrations, self.conf)

    def teardownall(self):
        for dbname in self.databases:
            self.teardown(dbname)

    def teardown(self, dbname):
        path = self.databases[dbname]['file']
        db = self.databases[dbname]['db']
        db.close()
        Database.drop(path)

    def __getattr__(self, name):
        try:
            return self.databases[name]['db']
        except KeyError:
            return object.__getattr__(self, name)
