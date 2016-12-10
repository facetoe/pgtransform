from lib.strategy import Strategy, AttributeStrategy
from lib.util import strat_print_success


class NotNullableStrategy(AttributeStrategy):
    SQL_SET_NOT_NULL = """
    ALTER TABLE "%(table_name)s" ALTER COLUMN "%(column_name)s" SET NOT NULL
    """

    SQL_SET_NULL = """
    ALTER TABLE "%(table_name)s" ALTER COLUMN "%(column_name)s" DROP NOT NULL
    """

    def __init__(self, **kwargs):
        Strategy.__init__(self, **kwargs)

    def execute(self, cursor, diff_node):
        column_info = self.get_column_info(diff_node)
        if column_info.expected is False:
            strat_print_success("Setting %(table_name)s.%(column_name)s to NOT NULL" % vars(column_info))
            cursor.execute(self.SQL_SET_NOT_NULL % vars(column_info))
        else:
            strat_print_success("Setting %(table_name)s.%(column_name)s to NULLABLE" % vars(column_info))
            cursor.execute(self.SQL_SET_NULL % vars(column_info))


    @property
    def target(self):
        return 'is_nullable'
