from random import Random
from PyDFlow.PyFun import future
import PyDFlow.base.rand as base
from functools import wraps

def rlift(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return (future(r) for r in f(*args, **kwargs))


genrandom = rlift(base.genrandom)
genrandint = rlift(base.genrandom)
gensample = rlift(base.gensample)