import subprocess
import sys
from contextlib import contextmanager

import pg8000


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


class Postgres(object):
    def __init__(self, db_name, schema_name, username, password):
        self.db_name = db_name
        self.schema_name = schema_name
        self.username = username
        self.password = password

        self.pgdata = None
        self.db_oid = None
        self.schema_oid = None
        self.table_oids = None
        self._connection = None

    @contextmanager
    def get_connection(self):
        if self._connection is None:
            pg8000.paramstyle = 'named'
            self._connection = pg8000.connect(user=self.username, database=self.db_name)
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

    def get_table_oids(self):
        stmt = 'select relname, oid from pg_class where relnamespace=:schema'
        results = self.execute(stmt, {'schema': self.schema_oid})
        self.table_oids = dict(results)

    def get_pgdata(self):
        self.pgdata = self.execute('show data_directory')[0][0]

    def get_schema_oid(self):
        stmt = 'select oid from pg_namespace where nspname=:schema'
        self.schema_oid = self.execute(stmt, {'schema': self.schema_name})[0][0]

    def get_db_oid(self):
        self.db_oid = self.execute('select oid from pg_database where datname=:db', {'db': self.db_name})[0][0]

    def refresh_info(self):
        self.get_pgdata()
        self.get_db_oid()
        self.get_schema_oid()
        self.get_table_oids()

    def get_table_ddl(self, table_name):
        return subprocess.check_output(['pg_dump', '-t', table_name, '--schema-only', self.db_name])

    def start_postgres(self):
        subprocess.check_call(['pg_ctl', '-D', self.pgdata, 'start'])

    def stop_postgres(self):
        subprocess.check_call(['pg_ctl', '-m', 'fast', '-D', self.pgdata, 'stop'])

    @staticmethod
    def clear_cache():
        if sys.platform == 'darwin':
            subprocess.check_call((['sudo', 'purge']))
        else:
            subprocess.check_call(['sync'])
            with open('/proc/sys/vm/drop_caches', 'w') as f:
                f.write('1')

    @contextmanager
    def cold(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self.stop_postgres()
        self.clear_cache()
        yield
        self.start_postgres()
