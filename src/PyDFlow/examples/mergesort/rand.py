#!/usr/bin/python

import random
import sys

if len(sys.argv) >= 2:
    n = int(sys.argv[1])
else:
    n = 100000

for i in range(n):
    print random.randint(1, 10000000)
