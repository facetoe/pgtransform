import sys
from collections import OrderedDict

import psycopg2
from psycopg2.extras import DictCursor

from lib.diff import DiffNode, DiffItem


class DBConnection(object):
    def __init__(self, host, database, user, password, port=5432, connect_timeout=30):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connect_timeout = connect_timeout
        self.db_connection = None

    def connect(self):
        conn = psycopg2.connect(database=self.database,
                                user=self.user,
                                password=self.password,
                                host=self.host,
                                port=self.port,
                                connect_timeout=self.connect_timeout)
        return conn

    @property
    def connection(self):
        if self.db_connection is None:
            self.db_connection = self.connect()
        return self.db_connection


class DatabaseObject(object):
    def construct(self, **kwargs):
        raise NotImplementedError("You must implement construct()!")

    def compare_to(self, obj):
        raise NotImplementedError("You must implement compare_to()!")

    def set_attributes(self, query_result, remap_attr_names=None, ignore_none=True):
        for attr_name, attr in query_result.iteritems():
            if ignore_none and attr is None:
                continue

            if remap_attr_names and attr_name in remap_attr_names:
                attr_name = remap_attr_names[attr_name]

            if attr == "NO":
                attr = False
            elif attr == "YES":
                attr = True

            setattr(self, attr_name, attr)

    def select_as_objects(self, cursor, sql_select, object_type, remap_attr_names, **kwargs):
        cursor.execute(sql_select, self.__dict__)
        objects = list()
        for obj in cursor.fetchall():
            object_dict = dict(obj)
            object_dict.update(dict(object_type=object_type, remap_attr_names=remap_attr_names))
            object_dict.update(kwargs)
            objects.append(object_type(**object_dict))
        return objects

    def compare_object(self, target_attribute, object_type, other_object):
        object_diff = DiffNode(name=self.name)
        for missing_target in self.get_missing(other_object, target_attribute):
            object_diff.append(
                DiffNode(name=missing_target.name,
                         object_type=object_type,
                         data=DiffItem(name=missing_target.name,
                                       expected=missing_target,
                                       found=None)))

        for extra_target in self.get_extra(other_object, target_attribute):
            object_diff.append(
                DiffNode(name=extra_target.name,
                         object_type=object_type,
                         data=DiffItem(name=extra_target.name,
                                       expected=None,
                                       found=extra_target)))

        for matching_target in self.get_matching(other_object, target_attribute):
            other_target = filter(lambda o: o.name == matching_target.name, getattr(other_object, target_attribute))[0]
            other_object_diff = matching_target.compare_to(other_target)
            if other_object_diff.isbranch():
                object_diff.append(other_object_diff)

        return object_diff

    def compare_attrs(self, other_obj, ignore_attr=None):
        attr_diffs = DiffNode(name=self.name)

        for attr_name, attr in self.__dict__.iteritems():
            if ignore_attr and attr_name == ignore_attr:
                continue

            if attr_name in other_obj.__dict__:
                other_attr = other_obj.__dict__[attr_name]
                if attr != other_attr:
                    attr_diffs.append(
                        DiffNode(name=attr_name,
                                 object_type=ColumnAttribute,
                                 expected_obj=self,
                                 data=DiffItem(name=attr_name,
                                               expected=attr,
                                               found=other_attr)))
            else:
                attr_diffs.append(
                    DiffNode(name=attr_name,
                             object_type=ColumnAttribute,
                             expected_obj=self,
                             data=DiffItem(name=attr_name,
                                           expected=attr,
                                           found=None)))

        for other_attr_name, other_attr in other_obj.__dict__.iteritems():
            if ignore_attr and other_attr_name == ignore_attr:
                continue

            if other_attr_name not in self.__dict__:
                attr_diffs.append(
                    DiffNode(name=other_attr_name,
                             object_type=ColumnAttribute,
                             expected_obj=self,
                             data=DiffItem(name=other_attr_name,
                                           expected=None,
                                           found=other_attr)))

        return attr_diffs

    def get_missing(self, obj, target_attr):
        return [o for o in getattr(self, target_attr) if o not in getattr(obj, target_attr)]

    def get_extra(self, obj, target_attr):
        return [o for o in getattr(obj, target_attr) if o not in getattr(self, target_attr)]

    def get_matching(self, obj, target_attr):
        return [o for o in getattr(self, target_attr) if o in getattr(obj, target_attr)]

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return self.name != other.name

    def __repr__(self):
        return "[%s: %s]" % (self.__class__.__name__, self.name)


class Database(DatabaseObject):
    SQL_CONSTRUCT = """
    SELECT table_name FROM information_schema.tables
    WHERE
      table_schema = %(schema_name)s
    AND
      NOT table_name ILIKE ANY (%(ignore_tables)s)
    """

    SQL_SELECT_PROCEDURES = """
    SELECT
      p.proname     AS name,
      p.pronargs    AS num_args,
      t1.typname    AS return_type,
      l.lanname     AS language_type,
      p.proargtypes AS argument_types_oids,
      prosrc        AS body
    FROM pg_proc p
      LEFT JOIN pg_type t1 ON p.prorettype = t1.oid
      LEFT JOIN pg_language l ON p.prolang = l.oid
    WHERE proname IN (
      SELECT routine_name
      FROM information_schema.routines
      WHERE specific_schema NOT IN
            ('pg_catalog', 'information_schema')
            AND type_udt_name != 'trigger'
            AND data_type = 'USER-DEFINED'
    )
    """

    def __init__(self, db_connection, ignore_columns=None, ignore_tables=None, schema_name='public'):
        self.name = db_connection.database
        self.ignore_columns = ignore_columns if ignore_columns else []
        self.ignore_tables = ignore_tables if ignore_tables else []
        self.schema_name = schema_name
        self.tables = list()
        self.procedures = list()
        self.construct(cursor=db_connection.connection.cursor(cursor_factory=DictCursor))

    def construct(self, **kwargs):
        cursor = kwargs['cursor']
        cursor.execute(self.SQL_CONSTRUCT, self.__dict__)
        self.tables.extend([Table(cursor, row['table_name'],
                                  self.name,
                                  ignore_columns=self.ignore_columns)
                            for row in cursor.fetchall()])
        self.procedures.extend(self.select_as_objects(cursor,
                                                      sql_select=self.SQL_SELECT_PROCEDURES,
                                                      object_type=Procedure,
                                                      remap_attr_names=dict()))

    def compare_to(self, other_database):
        db_diffs = DiffNode(name=self.name)
        table_diffs = self.compare_object('tables', Table, other_database)
        procedure_diffs = self.compare_object('procedures', Procedure, other_database)
        db_diffs.merge(procedure_diffs)
        db_diffs.merge(table_diffs)
        return db_diffs


class Table(DatabaseObject):
    SQL_CONSTRUCT = """
        SELECT *
        FROM information_schema.tables
        WHERE
          table_name = %(name)s
    """

    SQL_SELECT_COLUMNS = """
    SELECT
        column_name,
        data_type,
        udt_name,
        column_default,
        is_nullable,
        character_maximum_length,
        numeric_precision
    FROM
        information_schema.columns
    WHERE
        table_name = %(name)s
    AND
        table_catalog = %(database_name)s
    AND
        NOT column_name ILIKE ANY (%(ignore_columns)s)
    """

    SQL_SELECT_CONSTRAINTS = """
    SELECT c.conname AS constraint_name,
           CASE c.contype
           WHEN 'c' THEN 'CHECK'
           WHEN 'f' THEN 'FOREIGN KEY'
           WHEN 'p' THEN 'PRIMARY KEY'
           WHEN 'u' THEN 'UNIQUE'
           END AS "constraint_type",
           CASE WHEN c.condeferrable = 'f' THEN 'NO' ELSE 'YES' END AS is_deferrable,
           CASE WHEN c.condeferred = 'f' THEN 'NO' ELSE 'YES' END AS is_deferred,
           t.relname AS table_name,
           -- Not sure what this does but there are a lot of results -- array_to_string(c.conkey, ' ') AS constraint_key,
           CASE confupdtype
           WHEN 'a' THEN 'NO ACTION'
           WHEN 'r' THEN 'RESTRICT'
           WHEN 'c' THEN 'CASCADE'
           WHEN 'n' THEN 'SET NULL'
           WHEN 'd' THEN 'SET DEFAULT'
           END AS on_update,
           CASE confdeltype
           WHEN 'a' THEN 'NO ACTION'
           WHEN 'r' THEN 'RESTRICT'
           WHEN 'c' THEN 'CASCADE'
           WHEN 'n' THEN 'SET NULL'
           WHEN 'd' THEN 'SET DEFAULT'
           END AS on_delete,
           CASE confmatchtype
           WHEN 'u' THEN 'UNSPECIFIED'
           WHEN 'f' THEN 'FULL'
           WHEN 'p' THEN 'PARTIAL'
           END AS match_type,
           t2.relname AS references_table,
           array_to_string(c.confkey, ' ') AS fk_constraint_key
    FROM pg_constraint c
      LEFT JOIN pg_class t  ON c.conrelid  = t.oid
      LEFT JOIN pg_class t2 ON c.confrelid = t2.oid
    WHERE t.relname = %(name)s
    """

    SQL_SELECT_FOREIGN_KEYS = """
	SELECT tc.table_schema,
		tc.constraint_name,
		tc.table_name,
		kcu.column_name,
		ccu.table_name  AS foreign_table_name,
		ccu.column_name AS foreign_column_name
	FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
				JOIN information_schema.constraint_column_usage ccu
				ON ccu.constraint_name = tc.constraint_name
	WHERE constraint_type = 'FOREIGN KEY'
		AND
		ccu.table_name= %(name)s
	"""

    SQL_SELECT_TRIGGERS = """
    SELECT
      trigger_name,
      -- This causes false positives - event_manipulation,
      event_object_table,
      action_order,
      action_condition,
      action_statement,
      action_orientation,
      action_timing,
      action_reference_old_table,
      action_reference_new_table,
      action_reference_new_row,
      created
    FROM information_schema.triggers
    WHERE trigger_schema
          NOT IN ('pg_catalog', 'information_schema')
          AND
          event_object_table = %(name)s
    """

    SQL_SELECT_INDEXES = """
	   SELECT
		t.relname AS table_name,
		a.attname AS column_name
	FROM pg_class t,
		pg_class i,
		pg_index ix,
		pg_attribute a
	WHERE t.oid = ix.indrelid
		AND
		i.oid = ix.indexrelid
		AND
		a.attrelid = t.oid
		AND
		a.attnum = ANY (ix.indkey)
		AND
		t.relkind = 'r'
		AND
		t.relname = %(name)s
		AND
		NOT a.attname ILIKE ANY (%(ignore_columns)s)
    """

    def __init__(self, cursor, table_name, database_name, ignore_columns=None):
        self.name = table_name
        self.database_name = database_name
        self.ignore_columns = ignore_columns if ignore_columns else []
        self.columns = list()
        self.triggers = list()
        self.primary_keys = list()
        self.foreign_keys = list()
        self.check_constraints = list()
        self.unique_constraints = list()
        self.indexes = list()

        self.construct(cursor=cursor)

    def construct(self, **kwargs):
        cursor = kwargs['cursor']
        cursor.execute(self.SQL_CONSTRUCT, self.__dict__)
        self.set_attributes(cursor.fetchone())
        self.set_constraints(cursor)
        self.columns.extend(self.select_as_objects(cursor,
                                                   sql_select=self.SQL_SELECT_COLUMNS,
                                                   object_type=Column,
                                                   remap_attr_names=dict(column_name='name')))
        self.triggers.extend(self.select_as_objects(cursor,
                                                    sql_select=self.SQL_SELECT_TRIGGERS,
                                                    object_type=Trigger,
                                                    remap_attr_names=dict(trigger_name='name')))
        self.indexes.extend(self.select_as_objects(cursor,
                                                   sql_select=self.SQL_SELECT_INDEXES,
                                                   object_type=Index,
                                                   remap_attr_names=dict(
                                                       column_name='name')))  ## Indexes are compared based on the column they target as the names often differ
        self.foreign_keys.extend(self.select_as_objects(cursor,
                                                        sql_select=self.SQL_SELECT_FOREIGN_KEYS,
                                                        object_type=ForeignKey,
                                                        remap_attr_names=dict(constraint_name='name')))

    def set_constraints(self, cursor):
        cursor.execute(self.SQL_SELECT_CONSTRAINTS, self.__dict__)
        for const in cursor.fetchall():
            constraint = dict(const)
            constraint_type = constraint['constraint_type']
            constraint.update(dict(remap_attr_names=dict(constraint_name='name')))
            if constraint_type == 'CHECK':
                constraint.update(dict(object_type=CheckConstraint))
                self.check_constraints.append(CheckConstraint(**constraint))
            elif constraint_type == 'PRIMARY KEY':
                constraint.update(dict(object_type=PrimaryKey))
                self.primary_keys.append(PrimaryKey(**constraint))
            elif constraint_type == 'UNIQUE':
                constraint.update(dict(object_type=UniqueConstraint))
                self.unique_constraints.append(UniqueConstraint(**constraint))
            elif constraint_type == 'FOREIGN KEY':
                # We select more information about foreign keys in a separate query.
                continue
            else:
                sys.stderr.write("Unknown Constraint: %s\n" % constraint_type)

    def compare_to(self, other_table):
        db_objects = OrderedDict(columns=Column,
                                 check_constraints=CheckConstraint,
                                 primary_keys=PrimaryKey,
                                 foreign_keys=ForeignKey,
                                 unique_constraints=UniqueConstraint,
                                 triggers=Trigger,
                                 indexes=Index)
        table_diffs = DiffNode(self.name)
        for name, object_type in db_objects.iteritems():
            table_diffs.merge(self.compare_object(name, object_type, other_table))
        return table_diffs


class BaseTableAttribute(DatabaseObject):
    def __init__(self, **kwargs):
        self.object_type = kwargs['object_type']
        self.remap_attr_names = kwargs['remap_attr_names']
        self.ignore_attr = kwargs.pop('ignore_attr', None)
        self.construct(**kwargs)

    def construct(self, **kwargs):
        self.set_attributes(kwargs, remap_attr_names=self.remap_attr_names)

    def compare_to(self, other_column):
        return self.compare_attrs(other_column, ignore_attr=self.ignore_attr)


class Column(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class CheckConstraint(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class ForeignKey(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class PrimaryKey(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class UniqueConstraint(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class Trigger(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class Index(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class Procedure(BaseTableAttribute):
    def __init__(self, **kwargs):
        BaseTableAttribute.__init__(self, **kwargs)


class ColumnAttribute(DatabaseObject):
    def construct(self, **kwargs):
        raise NotImplemented()

    def compare_to(self, obj):
        raise NotImplemented()
