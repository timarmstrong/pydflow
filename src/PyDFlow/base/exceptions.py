'''
@author: Tim Armstrong
'''
class UnimplementedException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)

class NoDataException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)
    
class EmptyPlaceholderException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __repr__(self):
        return repr(self.parameter)
    

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
    
    def __str__(self):
        return self.__repr__()
    
    def add_exception(self, exception):
        self.causes.append(exception)
        
    def add_exceptions(self, exceptions):
        self.causes.extend(exceptions)