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