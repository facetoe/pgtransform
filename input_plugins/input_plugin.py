#!/usr/bin/env python
from collections import namedtuple

ConnectionConfig = namedtuple('ConnectionConfig', ['database', 'user', 'host', 'password', 'port'])


class InputPlugin(object):
    def get_connection_configs(self):
        raise NotImplementedError("You must implement get_connection_configs()!")
