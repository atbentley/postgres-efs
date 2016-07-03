import shutil
import subprocess

import sys
from contextlib import contextmanager

import time

import os

import pg8000


__version__ = '0.0.1'


def parse_script(script):
    statements = []
    current_statement = ''
    for line in script.split('\n'):
        if line.startswith('--'):
            continue
        current_statement += line
        if line.endswith(';'):
            statements.append(current_statement)
            current_statement = ''
    if current_statement.strip():
        statements.append(current_statement)
    return statements


class Pefs(object):
    def __init__(self, db_name, schema_name, efs_root, pg_user, pg_pass):
        self.db_name = db_name
        self.schema_name = schema_name
        self.efs_root = efs_root
        self.pg_user = pg_user
        self.pg_pass = pg_pass

        self.pgdata = None
        self.db_oid = None
        self.schema_oid = None
        self.tables = None
        self._connection = None

        self.get_pgdata()
        self.get_db_oid()
        self.get_schema_oid()
        self.get_tables()

    def get_tables(self):
        stmt = 'select relname, oid from pg_class where relnamespace=:schema'
        results = self.execute(stmt, {'schema': self.schema_oid})
        self.tables = dict(results)

    def get_pgdata(self):
        self.pgdata = self.execute('show data_directory')[0][0]

    def get_schema_oid(self):
        stmt = 'select oid from pg_namespace where nspname=:schema'
        self.schema_oid = self.execute(stmt, {'schema': self.schema_name})[0][0]

    def get_db_oid(self):
        self.db_oid = self.execute('select oid from pg_database where datname=:db', {'db': self.db_name})[0][0]

    @contextmanager
    def get_connection(self):
        if self._connection is None:
            pg8000.paramstyle = 'named'
            self._connection = pg8000.connect(user=self.pg_user, database=self.pg_pass)
        yield self._connection
        self._connection.commit()

    def execute(self, statement, args=()):
        with self.get_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(statement, args)
            result = cursor.fetchall()
        return result

    def execute_script(self, script):
        with self.get_connection() as connection:
            statements = parse_script(script)
            cursor = connection.cursor()
            for statement in statements:
                cursor.execute(statement)

    def get_table_ddl(self, table_name):
        return subprocess.check_output(['pg_dump', '-t', table_name, '--schema-only', self.db_name])

    def build_table_path(self, table_oid):
        return os.path.join(self.pgdata, 'base', str(self.db_oid), str(table_oid))

    def copy_table(self, table_name, table_oid):
        shutil.copy(self.build_table_path(table_oid), os.path.join(self.efs_root, self.db_name, 'data', table_name))

    def copy_ddl(self, table_name):
        with open(os.path.join(self.efs_root, self.db_name, 'ddl', table_name), 'w') as f:
            f.write(self.get_table_ddl(table_name))

    def start_postgres(self):
        subprocess.check_call(['pg_ctl', '-D', self.pgdata, 'start'])

    def stop_postgres(self):
        subprocess.check_call(['pg_ctl', '-m', 'fast', '-D', self.pgdata, 'stop'])

    def clear_cache(self):
        if sys.platform == 'darwin':
            subprocess.check_call((['sudo', 'purge']))
        else:
            subprocess.check_call(['sync'])
            with open('/proc/sys/vm/drop_caches', 'w') as f:
                f.write(1)

    @contextmanager
    def cold_postgres(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            time.sleep(0.5)
        self.stop_postgres()
        self.clear_cache()
        yield
        self.start_postgres()

    def clone_db(self):
        os.mkdir(os.path.join(self.efs_root, self.db_name))
        os.mkdir(os.path.join(self.efs_root, self.db_name, 'data'))
        os.mkdir(os.path.join(self.efs_root, self.db_name, 'ddl'))
        for table_name in self.tables:
            self.copy_ddl(table_name)

        with self.cold_postgres():
            for table_name, table_oid in self.tables.items():
                self.copy_table(table_name, table_oid)

    def link_db(self):
        for table_name in os.listdir(os.path.join(self.efs_root, self.db_name, 'ddl')):
            with open(os.path.join(self.efs_root, self.db_name, 'ddl', table_name), 'r') as f:
                ddl = f.read()
            self.execute_script(ddl)

        self.get_tables()

        with self.cold_postgres():
            for table_name, table_oid in self.tables.items():
                table_path = self.build_table_path(table_oid)
                os.unlink(table_path)
                os.symlink(os.path.join(self.efs_root, self.db_name, 'data', table_name), table_path)
