"""
Manage locations where we will search for app executables.

This module does not try to use the os's built in path mechanism, that is up to the
executor to decide if it wants to do that.  

TODO: only support local execution, want to extend with something like Swift's tc.data
mechanism.

TODO: it is worth thinking about just using the OS's resolution mechanism and manipulating the
PATH environment variable, but this is a bit ugly.
@author: Tim Armstrong
"""
from PyDFlow.util.args import is_iterable
import os.path

paths = [""]
cache = {}

def lookup(exec_name):
    res = cache.get(exec_name, None)
    if res is None:
        for p in paths:
            f = os.path.join(p, exec_name)
            if os.path.exists(f):
                return f
    else:
        return res
    return None 

def set_paths(new_paths):
    """
    Override the existing app path settings.
    """
    global cache
    cache = {}
    if is_iterable(new_paths):
        global paths
        paths = new_paths
    else:
        raise TypeError("Argument to set_app_paths must be iterable")

def add_path(path, top=True):
    """
    Add a new location to the app paths.
    if top is true, the new path will be given the 
    highest priority and therefore betried before all the
    existing paths. Otherwise, it will be given the 
    lowest priority
    """
    if top:
        # only need to flush cache if higher priority than existing
        global cache
        cache = {}
    global paths
    if is_iterable(path):
        if top:
            paths = list(path) + paths
        else:
            paths.extend(path)
    else:
        if top:
            paths.insert(0, path)
        else:
            paths.append(path)

