import cPickle as pickle
import functools
import inspect
import os
import sys
from imp import find_module
from types import ModuleType

from termcolor import colored, cprint


def get_subclasses(package, BaseClass):
    filename, path, description = find_module(package)
    modules = sorted(set(i.partition('.')[0]
                         for i in os.listdir(path)
                         if i.endswith(('.py', '.pyc', '.pyo'))
                         and not i.startswith('__init__.py')))
    pkg = __import__(package, fromlist=modules)
    for m in modules:
        module = getattr(pkg, m)
        if type(module) == ModuleType:
            for c in dir(module):
                klass = getattr(module, c)
                if inspect.isclass(klass) and issubclass(klass, BaseClass) and klass is not BaseClass:
                    yield klass


def unpickle_database(pickle_path):
    with open(pickle_path, 'rb') as pickle_file:
        return pickle.load(pickle_file)


def pickle_database(database, out_path):
    with open(out_path, 'wb') as out_file:
        pickle.dump(database, out_file, pickle.HIGHEST_PROTOCOL)


def synchronized(lock=None):
    """
    Decorator for performing synchronized method calls.
    """

    def _decorator(wrapped):
        @functools.wraps(wrapped)
        def _wrapper(*args, **kwargs):
            with lock:
                return wrapped(*args, **kwargs)

        return _wrapper

    return _decorator


def format_ignore(option, opt, value, parser):
    results = list()
    for item in value.split(','):
        results.append(item.replace('*', '%'))
    setattr(parser.values, option.dest, results)


def fail(message):
    cprint("\n** %s **\n" % message, 'red', attrs=['bold'], file=sys.stderr)
    sys.exit(1)


def strat_print_success(message):
    print colored('\t--> ', 'green') + colored(message, 'white')


def strat_print_fail(message):
    print colored('\t--x ', 'red', attrs=['bold']) + colored(message, 'white', attrs=['bold'])


def strat_print_warn(message):
    print colored('\t--s ', 'yellow', attrs=['bold']) + colored(message, 'white')


def print_warn(heading, message):
    print colored(heading, 'yellow', attrs=['bold']) + message


def print_info(heading, message=''):
    print colored(heading, 'white', attrs=['bold']) + message
