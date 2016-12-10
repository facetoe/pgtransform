#!/usr/bin/env python

import sys

import ansible.inventory
from ansible.constants import DEFAULT_HOST_LIST
from ansible.parsing.dataloader import DataLoader
from ansible.vars import VariableManager

from input_plugins.input_plugin import InputPlugin, ConnectionConfig


class AnsibleInput(InputPlugin):
    def __init__(self, pattern, inventory=DEFAULT_HOST_LIST):
        self.inventory = ansible.inventory.Inventory(loader=DataLoader(),
                                                     variable_manager=VariableManager(),
                                                     host_list=inventory)

        self.connections = self._get_connections(pattern)

    def get_connection_configs(self):
        return self.connections

    def _get_connections(self, pattern):
        connections = []
        for host in self.inventory.get_hosts(pattern):
            for site in host.vars['sites']:
                if 'db_credentials' in site and site['db_credentials']:
                    connections.append(self._get_connection(host, site))
                else:
                    sys.stderr.write("No db_credentials for: %s\n" % site['identifier'])
        return connections

    def _get_connection(self, host, site):
        dns = host.vars['dns']
        db_creds = site['db_credentials']
        return ConnectionConfig(db_creds['dbname'], db_creds['username'], dns, db_creds['password'], 5432)
