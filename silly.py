from PyDFlow.PyFun import *
Int = future.subtype()


@func((Int), (int))
def decorated(x):
    return decorated


print repr(decorated(2).get())
