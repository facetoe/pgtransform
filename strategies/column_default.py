#!/usr/bin/env python

from lib.strategy import Strategy, AttributeStrategy
from lib.util import strat_print_success, strat_print_warn


class ColumnDefaultStrategy(AttributeStrategy):
    SQL_DATATYPE = """
    ALTER TABLE ONLY "%(table_name)s" ALTER COLUMN "%(column_name)s" SET DEFAULT %(expected)s
    """

    def __init__(self, **kwargs):
        Strategy.__init__(self, **kwargs)

    def execute(self, cursor, diff_node):
        column_info = self.get_column_info(diff_node)
        if column_info.column_name != 'id':
            strat_print_success("SETTING %(table_name)s.%(column_name)s DEFAULT to %(expected)s" % vars(column_info))
            cursor.execute(self.SQL_DATATYPE % vars(column_info))
        else:
            strat_print_warn("SKIPPING %(table_name)s.%(column_name)s" % vars(column_info))

    @property
    def target(self):
        return 'column_default'
