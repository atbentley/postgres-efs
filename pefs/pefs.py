import os
import shutil

from .postgres import Postgres


class Pefs(object):
    def __init__(self, db_name, schema_name, efs_root, pg_user, pg_pass):
        self.efs_root = efs_root
        self.db = Postgres(db_name, schema_name, pg_user, pg_pass)

    def build_table_path(self, table_oid):
        return os.path.join(self.db.pgdata, 'base', str(self.db.db_oid), str(table_oid))

    def copy_table(self, table_name, table_oid):
        shutil.copy(self.build_table_path(table_oid), os.path.join(self.efs_root, self.db.db_name, 'data', table_name))

    def copy_ddl(self, table_name):
        with open(os.path.join(self.efs_root, self.db.db_name, 'ddl', table_name), 'w') as f:
            f.write(self.db.get_table_ddl(table_name))

    def clone_db(self):
        self.db.refresh_info()
        os.mkdir(os.path.join(self.efs_root, self.db.db_name))
        os.mkdir(os.path.join(self.efs_root, self.db.db_name, 'data'))
        os.mkdir(os.path.join(self.efs_root, self.db.db_name, 'ddl'))
        for table_name in self.db.table_oids:
            self.copy_ddl(table_name)

        with self.db.cold():
            for table_name, table_oid in self.db.table_oids.items():
                self.copy_table(table_name, table_oid)

    def link_db(self):
        self.db.refresh_info()
        for table_name in os.listdir(os.path.join(self.efs_root, self.db.db_name, 'ddl')):
            with open(os.path.join(self.efs_root, self.db.db_name, 'ddl', table_name), 'r') as f:
                ddl = f.read()
            self.db.execute_script(ddl)

        self.db.get_table_oids()

        with self.db.cold():
            for table_name, table_oid in self.db.table_oids.items():
                table_path = self.build_table_path(table_oid)
                os.unlink(table_path)
                os.symlink(os.path.join(self.efs_root, self.db.db_name, 'data', table_name), table_path)
