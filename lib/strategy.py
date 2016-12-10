from collections import namedtuple

from lib.exception import ConfigException
from lib.pg_objects import DatabaseObject
from lib.util import get_subclasses

ColumnInfo = namedtuple('ColumnInfo', ['table_name', 'column_name', 'expected', 'found'])


class Strategy(object):
    """
    Base class for strategies. Provides some convenience methods.
    """

    def __init__(self, applicable_tables=None, applicable_columns=None, target_attr=None, target_type=None):
        self.applicable_tables = applicable_tables or []
        self.applicable_columns = applicable_columns or []
        self.target_attr = target_attr
        self.target_type = self._get_type(target_type)

    def execute(self, cursor, diff_node):
        """
        Implement this method in Strategy subclasses. This method will be passed, one after the other,
        to each diff node that it applies to (based on the rules in lib/pg_transform.py)

        :param cursor: target database cursor
        :param diff_node: the diff node to execute upon
        :return:
        """
        raise NotImplementedError("You must implement execute()!")

    @property
    def target(self):
        """
        Subclasses need to implement this property to return the appropriate target. For example, if this
        strategy targets types, such as Index, return this type. Otherwise return the name of the targeted attribute,
        eg, udt_name
        """
        raise NotImplementedError("You must implement target!")

    @property
    def name(self):
        return self.__class__.__name__

    def get_column_info(self, diff_node):
        table_name = diff_node.parent.parent.name
        column_name = diff_node.parent.name
        expected = diff_node.data.expected
        found = diff_node.data.found
        return ColumnInfo(table_name, column_name, expected, found)

    def _get_type(self, target_type):
        if not target_type:
            return None
        for type in get_subclasses('lib', DatabaseObject):
            if type.__name__ == target_type:
                return type
        raise ConfigException("No such type to target: %s" % target_type)


class AttributeStrategy(Strategy):
    """
    An attribute strategy targets a particulare attribute name, eg udt_name or character_length.
    """


class TypeStrategy(Strategy):
    """
    A type strategy targets a particular type, eg Index or ForeignKey
    """
