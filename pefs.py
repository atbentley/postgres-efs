import shutil
import subprocess

import sys

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


def start_postgres(pgdata):
    subprocess.check_call(['pg_ctl', '-D', pgdata, 'start'])


def stop_postgres(pgdata):
    subprocess.check_call(['pg_ctl', '-D', pgdata, 'stop'])


def clear_cache():
    if sys.platform == 'darwin':
        subprocess.check_call((['sudo', 'purge']))
    else:
        subprocess.check_call(['sync'])
        with open('/proc/sys/vm/drop_caches', 'w') as f:
            f.write(1)


def clone_db(db_name, schema_name, efs_root, connection):
    pgdata = get_pgdata(connection)
    db_oid = get_db_oid(db_name, connection)
    db_path = os.path.join(efs_root, db_name)
    os.mkdir(db_path)
    os.mkdir(os.path.join(db_path, 'data'))
    os.mkdir(os.path.join(db_path, 'ddl'))
    tables = get_table_oid_map(schema_name, connection)
    for table_name in tables:
        with open(os.path.join(db_path, 'ddl', table_name), 'w') as f:
            f.write(get_table_ddl(db_name, table_name))
    connection.close()
    stop_postgres(pgdata)
    clear_cache()
    for table_name, table_oid in tables.items():
        copy_table(pgdata, db_oid, table_oid, table_name, db_path)

    start_postgres(pgdata)


def link_db(db_name, schema_name, efs_root, connection):
    # make tables
    for table_name in os.listdir(os.path.join(efs_root, db_name, 'ddl')):
        with open(os.path.join(efs_root, db_name, 'ddl', table_name), 'r') as f:
            ddl = f.read()
        statements = []
        current_statement = ''
        for line in ddl.split('\n'):
            if line.startswith('--'):
                continue
            current_statement += line
            if line.endswith(';'):
                statements.append(current_statement)
                current_statement = ''
        if current_statement.strip():
            statements.append(current_statement)
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
        connection.commit()

    # delete page file and link it to the efs
    pgdata = get_pgdata(connection)
    db_oid = get_db_oid(db_name, connection)
    tables = get_table_oid_map(schema_name, connection)
    connection.close()
    stop_postgres(pgdata)
    clear_cache()
    for table_name, table_oid in tables.items():
        table_path = build_table_path(pgdata, db_oid, table_oid)
        os.unlink(table_path)
        os.symlink(os.path.join(efs_root, db_name, 'data', table_name), table_path)
    start_postgres(pgdata)


def main(db_name, schema_name, efs_root, username, password):
    pg8000.paramstyle = 'named'
    connection = pg8000.connect(user=username, database=db_name)
    clone_db(db_name, schema_name, efs_root, connection)
    # link_db(db_name, schema_name, efs_root, connection)

if __name__ == '__main__':
    main('andrew', 'public', '/Users/andrew/pefs', 'andrew', '')
