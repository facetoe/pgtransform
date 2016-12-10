from lib.pg_objects import DBConnection
from lib.pg_objects import Database
from util import unpickle_database


class DBProvider(object):
    def get_database(self):
        raise NotImplementedError("You must implement get_database()!")


class PickleProvider(DBProvider):
    def __init__(self, pickle_path):
        self.database = unpickle_database(pickle_path)

    def get_database(self):
        return self.database


class DBConnectionProvider(DBProvider):
    def __init__(self, host, database, user, password, port=5432, ignore_columns=None,
                 ignore_tables=None):
        self.db_connection = DBConnection(host=host,
                                          database=database,
                                          user=user,
                                          password=password,
                                          port=port)
        self.database = Database(self.db_connection, ignore_columns=ignore_columns,
                                 ignore_tables=ignore_tables)

    def get_database(self):
        return self.database
