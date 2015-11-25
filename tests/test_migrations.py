import tempfile

import mock

from squery_lite import migrations as mod


def mock_module():
    f, path = tempfile.mkstemp()
    with open(path, 'w') as fd:
        fd.write("""
def up(db, conf):
    db.query("SELECT 'foo';")
        """)
    fd = open(path, 'r')
    return fd


def test_pack_version():
    assert mod.pack_version(1, 1) == 10001
    assert mod.pack_version(24, 32) == 240032
    assert mod.pack_version(6, 56) == 60056
    assert mod.pack_version(49, 2) == 490002
    assert mod.pack_version(0, 11) == 11
    assert mod.pack_version(23, 0) == 230000


def test_unpack_version():
    assert mod.unpack_version(10001) == (1, 1)
    assert mod.unpack_version(240032) == (24, 32)
    assert mod.unpack_version(60056) == (6, 56)
    assert mod.unpack_version(490002) == (49, 2)
    assert mod.unpack_version(11) == (0, 11)
    assert mod.unpack_version(230000) == (23, 0)


@mock.patch.object(mod.os, 'remove')
@mock.patch.object(mod.os.path, 'exists')
def test_drop_db(exists, remove):
    db = mock.Mock()
    db.connection.path = '/path/to/database'
    exists.return_value = True
    mod.drop_db(db)
    db.close.assert_called_once_with()
    db.reconnect.assert_called_once_with()
    calls = [mock.call('.'.join([db.connection.path, ext]))
             for ext in mod.SQLITE_EXTENSIONS]
    exists.assert_has_calls(calls)
    remove.assert_has_calls(calls)


@mock.patch.object(mod.os, 'listdir', autospec=True)
def test_get_mods_no_dupes(listdir):
    """ Listing modules does not return duplicates """
    listdir.return_value = ['01_01_test.py', '01_01_test.pyc', '01_02_test.py']
    mocked_pkg = mock.Mock()
    mocked_pkg.__path__ = ['foo']
    m = mod.get_mods(mocked_pkg)
    assert m == [('01_01_test', 1, 1), ('01_02_test', 1, 2)]


@mock.patch.object(mod.os, 'listdir', autospec=True)
def test_get_mods_order(listdir):
    """ Listing modules does not return duplicates """
    listdir.return_value = ['01_02_test.py', '02_01_new.py', '01_03_test.py',
                            '01_01_test.py']
    mocked_pkg = mock.Mock()
    mocked_pkg.__path__ = ['foo']
    m = mod.get_mods(mocked_pkg)
    assert m == [('01_01_test', 1, 1),
                 ('01_02_test', 1, 2),
                 ('01_03_test', 1, 3),
                 ('02_01_new', 2, 1)]


@mock.patch.object(mod.os, 'listdir', autospec=True)
def test_get_mods_ignores_non_migration_files(listdir):
    """ Listing modules will not return modules aren't migrations """
    listdir.return_value = ['__init__.py', 'foo.py', '01_01_test.py',
                            '01_02_test.py']
    mocked_pkg = mock.Mock()
    mocked_pkg.__path__ = ['foo']
    m = mod.get_mods(mocked_pkg)
    assert m == [('01_01_test', 1, 1), ('01_02_test', 1, 2)]


def test_get_new():
    """ Even if migrations are not in correct order, they are reordered """
    mods = [('01_01_test', 1, 1),
            ('01_02_test', 1, 2),
            ('01_03_test', 1, 3),
            ('02_01_test', 2, 1)]
    m = mod.get_new(mods, 1, 2)
    assert list(m) == [('01_02_test', 1, 2),
                       ('01_03_test', 1, 3),
                       ('02_01_test', 2, 1)]


@mock.patch.object(mod.importlib, 'import_module')
def test_load_mod(import_module):
    mocked_pkg = mock.Mock()
    mocked_pkg.__name__ = 'mypkg'
    mocked_mod = mock_module()
    import_module.return_value = mocked_mod
    assert mod.load_mod('01_test', mocked_pkg) is mocked_mod
    import_module.assert_called_once_with('mypkg.01_test', package='mypkg')


@mock.patch.object(mod, 'unpack_version')
def test_get_version(unpack_version):
    """ Returns migration version based on database table """
    db = mock.Mock()
    assert mod.get_version(db) == unpack_version.return_value
    db.query.assert_any_call(mod.GET_VERSION_SQL)
    unpack_version.assert_called_once_with(db.result.user_version)


@mock.patch.object(mod, 'drop_db')
def test_get_version_drop_db(drop_db):
    """ Version-tracking table is crated if it doesn't exist """
    db = mock.Mock()
    db.result.user_version = None
    assert mod.get_version(db) == (0, 0)
    drop_db.assert_called_once_with(db)


@mock.patch.object(mod, 'pack_version')
def test_set_version(pack_version):
    db = mock.Mock()
    pack_version.return_value = 20004
    mod.set_version(db, 2, 4)
    pack_version.assert_called_once_with(2, 4)
    sql = mod.SET_VERSION_SQL.format(version=pack_version.return_value)
    db.query.assert_called_once_with(sql)


@mock.patch.object(mod, 'set_version')
def test_run_migration(set_version):
    """ Running migration calls module's ``up()`` method """
    db = mock.Mock()
    db.transaction.return_value.__enter__ = lambda x: None
    db.transaction.return_value.__exit__ = lambda x, y, z, n: None
    m = mock.Mock()
    mod.run_migration(0, 1, db, m)
    m.up.assert_called_once_with(db, {})
    set_version.assert_called_once_with(db, 0, 1)


@mock.patch.object(mod, 'logging')
@mock.patch.object(mod, 'set_version')
@mock.patch.object(mod, 'get_version')
@mock.patch.object(mod, 'get_mods')
@mock.patch.object(mod, 'get_new')
@mock.patch.object(mod, 'load_mod')
@mock.patch.object(mod, 'run_migration')
@mock.patch.object(mod.importlib, 'import_module')
def test_migrate(import_module, run, load_mod, get_new, get_mods, get_version,
                 *ignored):
    mocked_pkg = mock.MagicMock()
    mocked_pkg.__name__ = 'mypkg.migrations.dbname'
    import_module.return_value = mocked_pkg
    get_version.return_value = (1, 2)
    get_mods.return_value = ['01_01_test',
                             '01_02_test',
                             '01_03_test',
                             '02_01_new']
    get_new.return_value = [('01_03_test', 1, 3), ('02_01_new', 2, 1)]
    load_mod.return_value = 'loaded mod'
    db = mock.Mock()
    mod.migrate(db, 'mypkg', {})
    import_module.assert_called_once_with('mypkg')
    get_version.assert_called_once_with(db)
    get_mods.assert_called_once_with(mocked_pkg)
    get_new.assert_called_once_with(get_mods.return_value, 1, 3)
    load_mod.assert_has_calls([mock.call('01_03_test', mocked_pkg),
                               mock.call('02_01_new', mocked_pkg)])
    run.assert_has_calls([mock.call(1, 3, db, load_mod.return_value, {}),
                          mock.call(2, 1, db, load_mod.return_value, {})])
    assert db.refresh_table_stats.called
