from unittest import TestSuite
from TestPyFun import TestPyFun
from TestFuture import TestFuture


alltests = TestSuite(tests=(TestPyFun, TestFuture))