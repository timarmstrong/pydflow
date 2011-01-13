"""
Implements the logical types of channels (ie the type of the data
being passed through them)
"""

class LogicalTypeError(ValueError):
    def __init__(self, *args, **kwargs):
        ValueError.__init__(self, *args, **kwargs)

class fltype(object):
    """
    The base logical type in PyDFlow.
    Subclasses don't need to call init.
    """
    @classmethod
    def subtype(cls):
        """
        Returns a new type which is a subtype of
        the current type.
        """
        class subtyped(cls):
            pass
        return subtyped

    @classmethod
    def isinstance(cls, swvar):
        return isinstance(swvar, cls)

    @classmethod
    def issubclassof(cls, ocls):
        return issubclass(cls, ocls)

