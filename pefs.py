import shutil
import subprocess

import os

import pg8000


__version__ = '0.0.1'


def get_pgdata(connection):
    cursor = connection.cursor()
    cursor.execute('show data_directory')
    return cursor.fetchone()[0]


def get_db_oid(db_name, connection):
    cursor = connection.cursor()
    cursor.execute('select oid from pg_database where datname=:db', {'db': db_name})
    return cursor.fetchone()[0]


def get_schema_oid(schema_name, connection):
    cursor = connection.cursor()
    cursor.execute('select oid from pg_namespace where nspname=:schema', {'schema': schema_name})
    return cursor.fetchone()[0]


def get_table_oid_map(schema_name, connection):
    schema_oid = get_schema_oid(schema_name, connection)
    cursor = connection.cursor()
    cursor.execute('select relname, oid from pg_class where relnamespace=:schema', {'schema': schema_oid})
    results = cursor.fetchall()
    return dict(results)


def get_table_ddl(db_name, table_name):
    return subprocess.check_output(['pg_dump', '-t', table_name, '--schema-only', db_name])


def build_table_path(pgdata, db_oid, table_oid):
    return os.path.join(pgdata, 'base', str(db_oid), str(table_oid))


def copy_table(pgdata, db_oid, table_oid, table_name, efs_root):
    shutil.copy(build_table_path(pgdata, db_oid, table_oid), os.path.join(efs_root, 'data', table_name))


def clone_db(db_name, schema_name, efs_root, connection):
    pgdata = get_pgdata(connection)
    db_oid = get_db_oid(db_name, connection)
    db_path = os.path.join(efs_root, db_name)
    os.mkdir(db_path)
    os.mkdir(os.path.join(db_path, 'data'))
    os.mkdir(os.path.join(db_path, 'ddl'))
    tables = get_table_oid_map(schema_name, connection)
    for table_name, table_oid in tables.items():
        copy_table(pgdata, db_oid, table_oid, table_name, db_path)
        with open(os.path.join(db_path, 'ddl', table_name), 'w') as f:
            f.write(get_table_ddl(db_name, table_name))


def main(db_name, schema_name, efs_root, username, password):
    pg8000.paramstyle = 'named'
    connection = pg8000.connect(user=username, database=db_name)
    clone_db(db_name, schema_name, efs_root, connection)


if __name__ == '__main__':
    main('andrew', 'public', '/Users/andrew/pefs', 'andrew', '')
