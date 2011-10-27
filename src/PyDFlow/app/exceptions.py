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
Created on Mar 31, 2011

@author: tga
'''

class ExitCodeException(Exception):
    def __init__(self, taskstr, exitcode):
        self.taskstr = taskstr
        self.exitcode = exitcode
        
    def __repr__(self):
        return ' '.join([self.taskstr, "failed with exit code", str(self.exitcode)])
    
class AppLaunchException(Exception):
    def __init__(self, taskstr, appname, oserror):
        self.taskstr = taskstr
        self.appname = appname
        self.oserror = oserror
    def __repr__(self):
        return "application %s failed to launch for task %s. OSerror was:\n%s" % (
                            self.appname, self.taskstr, self.oserror)