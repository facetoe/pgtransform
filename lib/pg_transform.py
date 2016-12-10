#!/usr/bin/env python
from lib.strategy import Strategy, AttributeStrategy, TypeStrategy
from lib.util import get_subclasses, print_info, print_warn


class PGTransform(object):
    """
    Transform the target database by executing the relevant strategies on it.
    """

    def __init__(self, test_dbconnection_provider, database_diffs, config, target_name):
        self.db_connection = test_dbconnection_provider.db_connection.connection
        self.database_diffs = database_diffs
        self.strategies = get_subclasses(package='strategies', BaseClass=Strategy)
        self.config = config
        self.target_attr_name = target_name

    def transform(self, commit=False):
        """
        For each strategy, loop over it's applicable nodes and call the strategy.execute() method on it.
        :param commit: whether or not to commit
        """

        print_info("Transforming: ", self.target_attr_name)
        for strategy_class in self.strategies:
            strategy_config = self.config.get_strategy(strategy_class.__name__)
            if strategy_config is not None:
                strategy = strategy_class(**strategy_config)
                print_info('Applying strategy: ', strategy_class.__name__)
                for diff_node in self.get_target_nodes(strategy):
                    self.apply_strategy(diff_node, strategy)
                print

        if commit:
            self.db_connection.commit()
            print_info("Changes committed!")
        else:
            print_warn("Dry run", " - nothing committed")
            self.db_connection.rollback()

    def apply_strategy(self, node, strategy):
        """
        Apply a strategy.
        :param node: the diff node to pass to execute()
        :param strategy: the strategy to call execute() on
        :return:
        """
        cursor = self.db_connection.cursor()
        strategy.execute(cursor, node)
        cursor.close()

    def get_target_nodes(self, strategy):
        """
        Return a list of nodes that this strategy applies to. If applicable_tables or applicable_columns has been
        specified in the config, only nodes from those columns and or tables will be returned.
        :param strategy: stratgey to retrieve nodes for
        :return:
        """
        if issubclass(strategy.__class__, AttributeStrategy):
            return self.get_matching_nodes(strategy, target=strategy.target, attribute='name')
        elif issubclass(strategy.__class__, TypeStrategy):
            return self.get_matching_nodes(strategy, target=strategy.target, attribute='object_type')
        else:
            raise Exception("target_attr and target_type are mutually exclusive!")

    def get_matching_nodes(self, strategy, target, attribute='object_type'):
        """
        Return all matching nodes for this strategy. A node is considered to match if it's target attribute
        is equal to the target parameter that is passed in (essentially getattr(somenode, attribute) == target)).
        :param strategy: the target to gather nodes for
        :param target: target to compare with. Can be a subclass of DatabaseObject or a ColumnAttribute name
        :param attribute: the name of the DiffNode attribute to compare with target.
        :return:
        """
        if strategy.applicable_tables:
            return self.get_targets_from_tables(strategy, target=target, attribute=attribute)
        elif strategy.applicable_columns:
            return self.get_targets_from_columns(strategy, target=target, attribute=attribute)
        else:
            return self.database_diffs.findall(target, attribute)

    def get_targets_from_tables(self, strategy, target, attribute):
        """
        Only return nodes that belong to the tables specified in applicable_tables
        """
        nodes = list()
        for table in strategy.applicable_tables:
            t = self.database_diffs.find(table)
            if t:
                if strategy.applicable_columns:
                    nodes.extend(
                        self.get_targets_from_columns(strategy, target=target, attribute=attribute, table_node=t))
                else:
                    nodes.extend(t.findall(target, attribute=attribute))
        return nodes

    def get_targets_from_columns(self, strategy, target, attribute, table_node=None):
        """
        Only return nodes that belong to the columns specified in applicable_columns
        """
        nodes = list()
        for column_name in strategy.applicable_columns:
            if table_node is not None:
                c = table_node.find(column_name)
                if c:
                    nodes.extend(c.findall(target, attribute=attribute))
            else:
                for diff_node in self.database_diffs.findall(target, attribute=attribute):
                    # parent.name is column_name
                    if diff_node.parent.name in strategy.applicable_columns:
                        nodes.append(diff_node)
        return nodes
