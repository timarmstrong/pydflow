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