"""
Implements the logical types of I-vars (ie the type of the data
being passed through them)
@author: Tim Armstrong
"""
# Shorthand for the passthrough type
_ = None

class LogicalTypeError(ValueError):
    def __init__(self, *args, **kwargs):
        ValueError.__init__(self, *args, **kwargs)

class flvar(object):
    """
    The base logical type in PyDFlow: a flow variable.
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
        subtyped.__name__ = cls.__name__ + ".subtype"
        return subtyped

    @classmethod
    def isinstance(cls, swvar):
        return isinstance(swvar, cls)

    @classmethod
    def issubclassof(cls, ocls):
        return issubclass(cls, ocls)


class Placeholder(object):
    def __init__(self, expected_class):
        self._expected_class = expected_class