from random import Random
from PyDFlow.PyFun import future
import PyDFlow.base.rand as base
from functools import wraps

genrandom = rlift(base.genrandom)
genrandint = rlift(base.genrandom)
gensample = rlift(base.gensample)

def rlift(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return (future.bind(r) for r in f(*args, **kwargs))



