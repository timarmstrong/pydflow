'''
@author: Tim Armstrong
'''

from random import Random
from PyDFlow.PyFun import py_ivar
import PyDFlow.base.rand as base
from functools import wraps

def rlift(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return (py_ivar(r) for r in f(*args, **kwargs))


genrandom = rlift(base.genrandom)
genrandint = rlift(base.genrandom)
gensample = rlift(base.gensample)