# Copyright 2010-2011 Tim Armstrong <tga@uchicago.edu>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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