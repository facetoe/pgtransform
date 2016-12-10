import json

from input_plugins.input_plugin import InputPlugin, ConnectionConfig


class JSONPlugin(InputPlugin):
    def __init__(self, path):
        self._json = json.load(open(path))

    def get_connection_configs(self):
        configs = []
        for database in self._json:
            configs.append(
                ConnectionConfig(database['name'], database['user'], database['host'], database['password'],
                                 database['port']))
        return configs
