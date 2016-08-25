import mock
import pytest

from squery_lite import squery as mod


MOD = mod.__name__


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_connection_object_connects(sqlite3):
    """ Connection object starts a connection """
    conn = mod.Connection('foo.db')
    sqlite3.connect.assert_called_once_with(
        'foo.db', detect_types=sqlite3.PARSE_DECLTYPES)
    assert conn._conn.isolation_level is None
    conn._conn.cursor().execute.assert_called_once_with(
        'PRAGMA journal_mode=WAL;')


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_connection_repr(*ignored):
    """ Connection object has human-readable repr """
    conn = mod.Connection('foo.db')
    assert repr(conn) == "<Connection path='foo.db'>"


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_connection_object_remebers_dbpath(sqlite3):
    """ Connection object can remember the database path """
    conn = mod.Connection('foo.db')
    assert conn.path == 'foo.db'


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_connection_has_sqlite3_connection_api(sqlite3):
    """ Connection object exposes sqlite3.Connection methods and props """
    conn = mod.Connection('foo.db')
    assert conn.cursor == sqlite3.connect().cursor
    assert conn.isolation_level == sqlite3.connect().isolation_level


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_connection_close(sqlite3):
    """ Connection object commits before closing """
    conn = mod.Connection('foo.db')
    conn.close()
    assert sqlite3.connect().commit.called
    assert sqlite3.connect().close.called


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_can_set_attributes_on_underlying_connection(sqlite3):
    """ Attributes set on the Connection instance are mirrored correctly """
    conn = mod.Connection('foo.db')
    conn.isolation_level = None
    assert conn.isolation_level == conn._conn.isolation_level
    conn.isolation_level = 'EXCLUSIVE'
    assert conn.isolation_level == conn._conn.isolation_level


@mock.patch(MOD + '.sqlite3', autospec=True)
def test_can_clone_connection(sqlite3):
    """ Duplicate connection objects can be created with new() method """
    conn = mod.Connection('foo.db')
    assert sqlite3.connect.call_count == 1
    conn2 = conn.new()
    assert sqlite3.connect.call_count == 2
    assert conn is not conn2
    assert conn.path == conn2.path


def test_registering_custom_function():
    """ Connection can register custom functions """

    def addtwo(s):
        return s + 2

    conn = mod.Connection(':memory:', funcs=[addtwo])
    cur = mod.Cursor(conn)
    cur.execute('create table foo(i)')
    cur.execute('insert into foo values (1)')
    cur.execute('insert into foo values (2)')
    cur.execute('insert into foo values (3)')
    cur.execute('insert into foo values (4)')
    cur.execute('insert into foo values (5)')
    cur.execute('select addtwo(i) as a from foo order by i')
    assert [r.a for r in cur] == [3, 4, 5, 6, 7]


def test_registering_custom_callable():
    """ Connection can register custom functions as callables """

    class AddTwo(object):
        def __call__(self, s):
            return s + 2

    conn = mod.Connection(':memory:', funcs=[AddTwo()])
    cur = mod.Cursor(conn)
    cur.execute('create table foo(i)')
    cur.execute('insert into foo values (1)')
    cur.execute('insert into foo values (2)')
    cur.execute('insert into foo values (3)')
    cur.execute('insert into foo values (4)')
    cur.execute('insert into foo values (5)')
    cur.execute('select addtwo(i) as a from foo order by i')
    assert [r.a for r in cur] == [3, 4, 5, 6, 7]


def test_registering_custom_aggregate():
    """ Connection can register custom aggregate """

    class Concat(object):
        def __init__(self):
            self.s = ''

        def step(self, s):
            self.s += str(s)

        def finalize(self):
            return self.s

    conn = mod.Connection(':memory:', aggregates=[Concat])
    cur = mod.Cursor(conn)
    cur.execute('create table foo(i)')
    cur.execute("insert into foo values ('a')")
    cur.execute("insert into foo values ('b')")
    cur.execute("insert into foo values ('c')")
    cur.execute("insert into foo values ('d')")
    cur.execute("insert into foo values ('e')")
    cur.execute("select concat(i) as a from foo order by i")
    assert cur.result.a == 'abcde'


@mock.patch(MOD + '.sqlite3')
def test_db_connect(sqlite3):
    mod.Database.connect('foo.db')
    sqlite3.connect.assert_called_once_with(
        'foo.db', detect_types=sqlite3.PARSE_DECLTYPES)


@mock.patch(MOD + '.sqlite3')
def test_db_uses_dbdict(sqlite3):
    """ The database will use a dbdict_factory for all rows """
    conn = mod.Database.connect('foo.db')
    assert conn.row_factory == mod.Row


@mock.patch(MOD + '.sqlite3')
def test_init_db_with_connection(*ignored):
    """ Database object is initialized with a connection """
    conn = mock.Mock()
    db = mod.Database(conn)
    assert db.conn == conn


@mock.patch(MOD + '.sqlite3')
def test_get_cursor(*ignored):
    """ Obtaining curor should return connection's cursor object """
    db = mod.Database(mock.Mock())
    cur = db.cursor()
    assert cur.cursor == db.conn.cursor.return_value


@mock.patch(MOD + '.sqlite3')
def test_get_curor_only_retrieved_once(sqlite3):
    """ Cursor is retrieved every time """
    db = mod.Database(mock.Mock())
    db.cursor()
    db.cursor()
    assert db.conn.cursor.call_count == 2


@mock.patch(MOD + '.sqlite3')
def test_convert_sqlbuilder_class_to_repr(*ignored):
    """ When sqlbuilder object is passed as query, it's converted to repr """

    @mod.convert_query
    def with_query(self, q):
        return q

    self = mock.Mock()  # because convert_query is a method deco
    select = mock.Mock(spec=mod.Select)
    select.serialize.return_value = 'SELECT * FROM foo;'
    sql = with_query(self, select)
    assert sql == select.serialize.return_value


@mock.patch(MOD + '.sqlite3')
def test_convert_string_query(*ignored):
    """ When raw SQL sting is passed, it's not conveted """

    @mod.convert_query
    def with_query(self, q):
        return q

    s = 'foobar'
    self = mock.Mock()  # because convert_query is a method deco
    sql = with_query(self, s)
    assert s is sql


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_query(*ignored):
    """ query() should execute a database query """
    db = mod.Database(mock.Mock())
    cursor = db.query('SELECT * FROM foo;')
    cursor.query.assert_called_once_with('SELECT * FROM foo;')


@mock.patch(MOD + '.sqlite3')
def test_query_execute(*ignored):
    """ query() should execute a database query """
    db = mod.Database(mock.Mock())
    cursor = db.query('SELECT * FROM foo;')
    cursor.cursor.execute.assert_called_once_with('SELECT * FROM foo;', {})


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_query_params(*ignored):
    """ Query converts positional arguments to params list """
    db = mod.Database(mock.Mock())
    cursor = db.query('SELECT * FROM foo WHERE bar = ?;', 12)
    cursor.query.assert_called_once_with(
        'SELECT * FROM foo WHERE bar = ?;', 12)


@mock.patch(MOD + '.sqlite3')
def test_query_params_execute(*ignored):
    """ Query converts positional arguments to params list """
    db = mod.Database(mock.Mock())
    cursor = db.query('SELECT * FROM foo WHERE bar = ?;', 12)
    cursor.cursor.execute.assert_called_once_with(
        'SELECT * FROM foo WHERE bar = ?;', (12,))


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_query_keyword_params(*ignored):
    """ Query converts keyword params into dict """
    db = mod.Database(mock.Mock())
    cursor = db.query('SELECT * FROM foo WHERE bar = :bar;', bar=12)
    cursor.query.assert_called_once_with(
        'SELECT * FROM foo WHERE bar = :bar;', bar=12)


@mock.patch(MOD + '.sqlite3')
def test_query_keyword_params_execute(*ignored):
    """ Query converts keyword params into dict """
    db = mod.Database(mock.Mock())
    cursor = db.query('SELECT * FROM foo WHERE bar = :bar;', bar=12)
    cursor.cursor.execute.assert_called_once_with(
        'SELECT * FROM foo WHERE bar = :bar;', {'bar': 12})


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_execute_alias(*ignored):
    """ Instace has execute() alias for cursor.execute() """
    db = mod.Database(mock.Mock())
    cursor = db.execute('SELECT * FROM foo WHERE bar = ?;', (12,))
    cursor.execute.assert_called_once_with(
        'SELECT * FROM foo WHERE bar = ?;', (12,))


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_executemany_alias(*ignored):
    """ Instance has executemany() alias for cursor.executemany() """
    db = mod.Database(mock.Mock())
    cursor = db.executemany('INSERT INTO foo VALUES (?, ?);', [(1, 2), (3, 4)])
    cursor.executemany.assert_called_once_with(
        'INSERT INTO foo VALUES (?, ?);', [(1, 2), (3, 4)])


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_executescript_alias(*ignored):
    """ Instace has executescript() alias for cursor.executescript() """
    db = mod.Database(mock.Mock())
    cursor = db.executescript('SELECT * FROM foo;')
    cursor.executescript.assert_called_once_with('SELECT * FROM foo;')


@mock.patch(MOD + '.sqlite3')
def test_commit_alias(sqlite3):
    """ Instance has commit() alias for connection.commit() """
    db = mod.Database(mock.Mock())
    db.commit()
    assert db.conn.commit.called


@mock.patch(MOD + '.sqlite3')
def test_rollback_alias(sqlite3):
    """ Instance has rollback() alias for connection.rollback() """
    db = mod.Database(mock.Mock())
    db.rollback()
    assert db.conn.rollback.called
    assert db.conn.commit.called


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_refresh_table_stats(*ignored):
    """ Instance can call ANALYZE """
    db = mod.Database(mock.Mock())
    cursor = db.refresh_table_stats()
    cursor.execute.assert_called_once_with('ANALYZE sqlite_master;')


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_acquire_lock(*ignored):
    """ Instance has a method for acquiring exclusive lock """
    db = mod.Database(mock.Mock())
    cursor = db.acquire_lock()
    cursor.execute.assert_called_once_with('BEGIN EXCLUSIVE;')


@mock.patch(MOD + '.sqlite3')
def test_results(*ignored):
    """ Results property gives access to cursor.fetchall() results """
    cursor = mod.Cursor(mock.Mock())
    res = cursor.results
    assert cursor.cursor.fetchall.called
    assert res == cursor.cursor.fetchall.return_value


@mock.patch(MOD + '.sqlite3')
def test_result(*ignored):
    """ Result property gives access to cursor.fetchone() resutls """
    cursor = mod.Cursor(mock.Mock())
    res = cursor.result
    assert cursor.cursor.fetchone.called
    assert res == cursor.cursor.fetchone.return_value


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_transaction(*ignored):
    """ Instance has a transaction context manager """
    db = mod.Database(mock.Mock())
    with db.transaction() as cur:
        cur.execute.assert_called_once_with('BEGIN;')
    assert cur.conn.commit.called


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.Cursor')
def test_transaction_exclusive(*ignored):
    """ Transactions can be exclusive """
    db = mod.Database(mock.Mock())
    with db.transaction(exclusive=True) as cur:
        cur.execute.assert_called_once_with('BEGIN EXCLUSIVE;')


@mock.patch(MOD + '.sqlite3')
def transaction_rollback(*ignored):
    """ Transactions rolls back on exception """
    db = mod.Database(mock.Mock())
    try:
        with db.transaction() as cur:
            raise RuntimeError()
        assert False, 'Expected to raise'
    except RuntimeError:
        assert cur.conn.rollback.called


@mock.patch(MOD + '.sqlite3')
def test_transaction_silent_rollback(*ignored):
    """ Transaction silently rolled back if silent flag is passed """
    db = mod.Database(mock.Mock())
    try:
        with db.transaction(silent=True):
            raise RuntimeError()
        assert db.conn.rollback.called
    except RuntimeError:
        assert False, 'Expected not to raise'


def test_transaction_with_new_connection():
    """ Transaction can use a new connection """
    db = mod.Database(mod.Connection(':memory:'))
    with db.transaction() as cur:
        assert db.conn is cur.conn
    with db.transaction(new_connection=True) as cur:
        assert db.conn is not cur.conn
        assert cur.conn.path == db.conn.path


def test_transaction_with_new_connection_closes():
    db = mod.Database(mod.Connection(':memory:'))
    with db.transaction() as cur:
        conn1 = cur.conn
    conn1.cursor()
    with db.transaction(new_connection=True) as cur:
        conn2 = cur.conn
    with pytest.raises(mod.sqlite3.ProgrammingError):
        conn2.cursor()


@mock.patch(MOD + '.sqlite3')
def test_database_repr(*ignored):
    """ Transaction has a human-readable repr """
    conn = mock.Mock()
    conn.path = 'foo.db'
    db = mod.Database(conn)
    assert repr(db) == "<Database connection='foo.db'>"


def test_row_factory():
    """ Factory should create a tuple w/ subscript and attr access """
    # We are doing this test without mocking because of the squery.Row subclass
    # of sqlite3.Row, which is implemented in C and not particularly friendly
    # to mock objects.
    conn = mod.Database.connect(':memory:')
    db = mod.Database(conn)
    db.query('create table foo (bar integer);')
    db.query('insert into foo values (1);')
    cursor = db.query('select * from foo;')
    res = cursor.result
    assert res.get('bar') == res.bar == res[0] == res['bar'] == 1
    assert res.keys() == ['bar']
    assert 'bar' in res
    assert res.get('missing', 'def') == 'def'


def test_row_factory_unicode_key():
    """ Factory should handle unicode keys correctly when using .get() """
    conn = mod.Database.connect(':memory:')
    db = mod.Database(conn)
    db.query('create table foo (bar integer);')
    db.query('insert into foo values (1);')
    cursor = db.query('select * from foo;')
    res = cursor.result
    assert res.get(u'bar', 'def') == 1


@mock.patch(MOD + '.sqlite3')
@mock.patch(MOD + '.logging')
def test_debug_printing(mock_logging, *ignored):
    db = mod.Database(mock.Mock(), debug=False)
    db.query('SELECT * FROM foo;')
    assert mock_logging.debug.called is False
    db.debug = True
    db.query('SELECT * FROM foo;')
    mock_logging.debug.assert_called_once_with('SQL: %s', 'SELECT * FROM foo;')


def test_cursor_iteration():
    conn = mod.Database.connect(':memory:')
    db = mod.Database(conn)
    db.query('create table foo (bar integer);')
    db.query('insert into foo values (1), (2), (3), (4);')
    cursor = db.query('select * from foo order by bar asc;')
    accumulate = []
    for res in cursor:
        accumulate.append(res.bar)
    assert accumulate == [1, 2, 3, 4]
