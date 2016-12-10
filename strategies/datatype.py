from lib.strategy import Strategy, AttributeStrategy
from lib.util import strat_print_success


class DatatypeStrategy(AttributeStrategy):
    SQL_DATATYPE = """
    ALTER TABLE "%(table_name)s" alter column "%(column_name)s" type %(expected)s
    """

    def __init__(self, **kwargs):
        Strategy.__init__(self, **kwargs)

    def execute(self, cursor, diff_node):
        column_info = self.get_column_info(diff_node)
        strat_print_success("SETTING %(table_name)s.%(column_name)s type to %(expected)s" % vars(column_info))
        cursor.execute(self.SQL_DATATYPE % vars(column_info))

    @property
    def target(self):
        return 'udt_name'
