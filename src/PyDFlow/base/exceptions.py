'''
@author: Tim Armstrong
'''
class UnimplementedException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)
    __str__ = __repr__

class NoDataException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)
    __str__ = __repr__
    
class EmptyPlaceholderException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)
    __str__ = __repr__
    

class InvalidReplaceException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)
    __str__ = __repr__
class ExecutionException(Exception):
    """
    A set of exceptions
    """
    def __init__(self, exceptions):
        causes = []
        exceptions=list(exceptions)
        while len(exceptions) > 0:
            e = exceptions.pop()
            if isinstance(e, ExecutionException):
                exceptions.extend(e.causes)
            else:
                causes.append(e)
                
        self.causes = causes
    def __repr__(self):
        return 'Execution Exception caused by: \n' + ('\n\n'.join([repr(ex) for ex in self.causes]))
    
    __str__ = __repr__
    
    def add_exception(self, exception):
        self.causes.append(exception)
        
    def add_exceptions(self, exceptions):
        self.causes.extend(exceptions)