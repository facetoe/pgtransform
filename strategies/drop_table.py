from lib.pg_objects import Table
from lib.strategy import Strategy, TypeStrategy
from lib.util import strat_print_success


class DropTableStrategy(TypeStrategy):
    SQL_DROP_TABLE = """
    DROP TABLE "%(name)s" CASCADE
    """

    def __init__(self, **kwargs):
        Strategy.__init__(self, **kwargs)

    def execute(self, cursor, diff_node):
        strat_print_success("Dropping table %(name)s" % vars(diff_node))
        cursor.execute(self.SQL_DROP_TABLE % vars(diff_node))

    @property
    def target(self):
        return Table
