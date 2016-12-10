import ConfigParser
import inspect
from ConfigParser import NoSectionError

from input_plugins import get_input_plugin
from lib.exception import ConfigException


class Config(object):
    _strategy_vars = ['applicable_tables', 'applicable_columns']

    _INPUT_PLUGIN = 'InputPlugin'
    required_sections = [_INPUT_PLUGIN]

    def __init__(self, path):
        self.config = ConfigParser.ConfigParser()
        self.config.read(path)
        self.check_config()

    def check_config(self):
        if not all([section in self.config.sections() for section in self.required_sections]):
            raise NoSectionError("%s is required!" % ",".join(self.required_sections))

    @property
    def input_plugin(self):
        plugin_args = dict()
        for arg_name in self.config.options(self._INPUT_PLUGIN):
            plugin_args[arg_name] = self.config.get(self._INPUT_PLUGIN, arg_name)

        class_name = plugin_args.pop('class_name', None)
        if not class_name:
            raise ConfigParser.NoOptionError("class_name", self._INPUT_PLUGIN)

        Plugin = get_input_plugin(class_name)
        if not Plugin:
            raise ConfigException("No such plugin: %s" % class_name)

        try:
            return Plugin(**plugin_args)
        except TypeError, e:
            raise ConfigException("Input plugin __init__ missing one of: %s" % self.get_missing_args(Plugin), e)

    def get_missing_args(self, Plugin):
        init_args = inspect.getargspec(Plugin.__init__)
        return [arg for arg in init_args.args
                if arg != 'self' and
                arg not in dict(zip(reversed(init_args.args or []), reversed(init_args.defaults or [])))]

    @property
    def strategies(self):
        strategies = list()
        for strategy in [s for s in self.config.sections() if s not in self.required_sections]:
            strategies.append(self.get_strategy(strategy))
        return strategies

    def get_strategy(self, section):
        strategy = dict()
        if not self.config.has_option(section, 'enabled') or self.config.get(section,
                                                                             'enabled').lower().strip() not in (
        'yes', 'true'):
            return None

        for var_name in self._strategy_vars:
            if self.config.has_option(section, var_name):
                setting = self.config.get(section, var_name)
                strategy[var_name] = self.split_lists(setting)
        return strategy

    def split_lists(self, setting):
        if ',' in setting:
            return map(lambda x: x.strip(), setting.split(','))
        else:
            return [setting]
