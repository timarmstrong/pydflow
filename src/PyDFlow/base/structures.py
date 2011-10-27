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

class IStruct(object):
    """
    Can be initialized bound by providing type + iterable:
        e.g. IStruct(future, [1,2,3])
    Can be initialized unbound by providing type and length
        E.g. IStruct(future, 3)
    """
    def __init__(self, type, init):
        if isinstance(init, (int, long)):
            list = [type() for i in range(init)]
        else:
            try:
                list = [type(x) for x in init]
            except TypeError:
                raise TypeError("IStruct must be initialized with integral number or an iterable")
        
        super(IStruct, self).__setattr__("_list", list)
        
    def __getitem__(self, key):
        if not isinstance(key, ( int, long, slice ) ):
            raise TypeError("Key should be integral or slice")
        return self._list[key]
    
    def __setitem__(self, key, value):
        raise TypeError("Setting item in IStruct is not supported")
    
    def __len__(self):
        return len(self._list)
    
    def __iter__(self):
        return iter(self._list)