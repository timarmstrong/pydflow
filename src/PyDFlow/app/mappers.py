'''
Created on Mar 15, 2011

@author: tga
'''
from itertools import chain
import glob
import re

class SimpleMapper(object):
    def __init__(self, type, prefix, suffix):
        super(SimpleMapper, self).__setattr__('_type', type)
        super(SimpleMapper, self).__setattr__('_prefix', prefix)
        super(SimpleMapper, self).__setattr__('_suffix', suffix)
        super(SimpleMapper, self).__setattr__('_indexed_items', {})
        super(SimpleMapper, self).__setattr__('_named_items', {})
        
    def __getitem__(self, key):
        if not isinstance(key, ( int, long ) ):
            raise TypeError("Key should be integral")
        item = self._indexed_items.get(key, None)
        if item is None:
            # Generate filename
            filename = "%s%d%s" % (self._prefix, key, self._suffix)
            item = self._type(filename)
            self._indexed_items[key] = item
        return item 
        
    def __setitem__(self, key, value):
        if isinstance(key, ( int, long ) ):
            #raise TypeError("Key should be integral")
            if self._indexed_items.get(key, None) is value:
                # No change: support this to allow use of <<=
                return
        raise TypeError("Setting item in SimpleMapper is not supported, use << operator instead")
    
    def __getattr__(self, name):
        item = self._named_items.get(name, None)
        if item is None:
            filename = "%s%s%s" % (self._prefix, name, self._suffix)
            item = self._type(filename)
            self._named_items[name] = item
        return item
        
    
    def __setattr__(self, name, value):
        if self._named_items.get(name, None) is value:
            # No change: support this to allow use of <<=
            return
        raise TypeError("Setting SimpleMapper attribute is not supported, use << operator instead")
    
    def __len__(self):
        return len(self._indexed_items) + len(self._named_items)
    
    def __iter__(self):
        return chain(self._indexed_items.itervalues(), self._named_items.itervalues())
    
    
class ReadOnlyArray(object):
    def __init__(self, list):
        super(ReadOnlyArray, self).__setattr__("_list", list)
        
    def __getitem__(self, key):
        if not isinstance(key, ( int, long ) ):
            raise TypeError("Key should be integral")
        return self._list[key]
    
    def __setitem__(self, key, value):
        raise TypeError("Setting item in ReadOnlyArray is not supported")
    
    def __len__(self):
        return len(self._list)
    
    def __iter__(self):
        return iter(self._list)
        
def GlobMapper(type, pattern):
    files = glob.glob(pattern)
    return ReadOnlyArray([type(f) for f in files])

def SubMapper(type, source, match, transform):
    compiled = re.compile(match)
    names = (re.sub(compiled, transform, f._bound) for f in source)
    return ReadOnlyArray([type(n) for n in names])
