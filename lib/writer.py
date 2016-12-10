#!/usr/bin/env python


class OutputWriter(object):
    def write(self, database_diffs):
        raise NotImplementedError("You must implement write()!")

    def visit(self, node, func, **func_kwargs):
        for child in node.children:
            if child.isleaf():
                func(child, **func_kwargs)
            else:
                self.visit(child, func, **func_kwargs)


class STDOUTWriter(OutputWriter):
    """
    Prints the difference tree to STDOUT
    """
    def write(self, database_diffs):
        if len(database_diffs) > 0:
            print database_diffs.to_tree()


class SQLightWriter(OutputWriter):
    """
    Writes the difference tree to a SQLite database.
    """
    SQL_CREATE_TABLE_DATABASE = """
    CREATE TABLE if not exists database(
           id         INTEGER PRIMARY KEY NOT NULL,
           name    TEXT NOT NULL
    )
    """

    SQL_SELECT_DATABASE = """
    SELECT id FROM database WHERE name == :name
    """

    SQL_INSERT_DATABASE = """
    INSERT INTO database (name) values (:name)
    """

    SQL_CREATE_TABLE_DIFFERENCES = """
    CREATE TABLE IF NOT EXISTS differences (
        id INTEGER PRIMARY KEY NOT NULL,
        table_name TEXT NOT NULL,
        path TEXT NOT NULL,
        type TEXT NOT NULL,
        expected TEXT,
        found TEXT,
        database INT NOT NULL,
        FOREIGN KEY(database) REFERENCES database(id)
    )
    """

    SQL_INSERT_DIFFERENCES = """
    INSERT INTO differences
    (table_name, path, type, expected, found, database)
    VALUES
    (:table_name, :path, :type, :expected, :found, :database)
    """

    SQL_DELETE_DIFFERENCES = """
    DELETE FROM differences WHERE database = :database_id
    """

    def __init__(self, db_path, db_name):
        import sqlite3

        self.db_name = db_name
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row

    def write(self, database_diffs):
        cursor = self.connection.cursor()
        cursor.execute(self.SQL_CREATE_TABLE_DATABASE)
        cursor.execute(self.SQL_CREATE_TABLE_DIFFERENCES)

        def insert_differences(node, cursor=None, database_id=None):
            path, table_name = self.get_node_path_name(node)
            expected = repr(node.data.expected) if node.data.expected else None
            found = repr(node.data.found) if node.data.found else None
            cursor.execute(self.SQL_INSERT_DIFFERENCES, {'table_name': table_name,
                                                         'path': path,
                                                         'name': node.name,
                                                         'type': node.object_type.__name__,
                                                         'expected': expected,
                                                         'found': found,
                                                         'database': database_id})

        database_id = self.upsert_database(cursor, self.db_name)
        cursor.execute(self.SQL_DELETE_DIFFERENCES, {'database_id': database_id})
        self.visit(database_diffs, insert_differences, cursor=cursor, database_id=database_id)
        self.connection.commit()

    def upsert_database(self, cursor, database_name):
        cursor.execute(self.SQL_SELECT_DATABASE, {'name': database_name})
        result = cursor.fetchone()
        if not result:
            cursor.execute(self.SQL_INSERT_DATABASE, {'name': database_name})
            cursor.execute(self.SQL_SELECT_DATABASE, {'name': database_name})
            result = cursor.fetchone()
        return result['id']

    def get_node_path_name(self, node):
        segments = [node.name]
        while node.parent.parent:
            node = node.parent
            segments.append(node.name)
        return ".".join(reversed(segments)), node.name
