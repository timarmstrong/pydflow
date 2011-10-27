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
from random import Random

def gen(f, seed=None):
    """
    Generate an unlimited supply of random numbers.
    Designed to be used with imap or izip.
    """
    r = Random()
    r.seed(seed)
    while True:
        yield f(r)
        
def genrandom(*args, **kwargs):
    return gen(lambda r: r.random(), *args, **kwargs)

def genrandint(a, b, *args, **kwargs):
    return gen(lambda r: r.randint(a, b), *args, **kwargs)

def gensample(population, k, *args, **kwargs):
    return gen(lambda r: r.sample(population, k), *args, **kwargs)

