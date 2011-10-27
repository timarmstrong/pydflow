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

from PyDFlow.app import *
from PyDFlow.PyFun import *
from PyDFlow import resultset, Multiple
import itertools
import datetime

n = 500
@app((localfile), ())
def noop():
    return App("touch", outfiles[0])


@app((localfile), (Multiple(localfile)))
def merge(*inf):
    return App("touch", outfiles[0])

res = [noop() for i in range(n)]

start_t = datetime.datetime.now()
for r in res:
    r.force()

i = 0
for r in res:
    print i, 
    r.get()
    i += 1
print ''

end_t = datetime.datetime.now()

print "%d app tasks  took %s" % (n, end_t - start_t)
@func((py_ivar), ())
def noopf():
    return 1
@func((py_ivar), (Multiple(py_ivar)))
def mergef(*x):
    return x[0]

"""
res = [noopf() for i in range(n)]

start_t = datetime.datetime.now()
for r in res:
    r.force()

i = 0
for r in res:
    print i, 
    r.get()
    i += 1
print ''

end_t = datetime.datetime.now()

print "%d func tasks  took %s" % (n, end_t - start_t)
"""

res = merge(*[noop() for i in range(n)])
start_t = datetime.datetime.now()
res.get()
end_t = datetime.datetime.now()
print "%d app tasks started together took %s" % (n, end_t - start_t)
"""
res = mergef(*[noopf() for i in range(n)])
start_t = datetime.datetime.now()
res.get()
end_t = datetime.datetime.now()
print "%d func tasks started together took %s" % (n, end_t - start_t)
"""
