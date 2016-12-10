#!/usr/bin/env python
from input_plugins.input_plugin import InputPlugin
from lib.util import get_subclasses


def get_input_plugin(class_name):
    for plugin in get_subclasses('input_plugins', InputPlugin):
        if plugin.__name__ == class_name:
            return plugin
