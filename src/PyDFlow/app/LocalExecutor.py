'''
@author: Tim Armstrong
'''
from __future__ import with_statement
from PyDFlow.app.exceptions import ExitCodeException, AppLaunchException
import time
"""
Refactoring plan:

request queue which receives tasks to run.
notification queue which sends back notifications
notification thread running in this process which calls continuations.

    This server could run in a separate process using the multiprocessing module.
    MAX_RUNNING worker threads which correspond to forked processes.
    
    
"""

from PyDFlow.base.mutex import graph_mutex

import subprocess
import threading
import Queue
import logging
import paths

#TODO: no polling?
POLL_INTERVAL = 0.05 # seconds between polling tasks when active
#TODO: get info about number of processors
MAX_RUNNING = 2

LIFO = False

structure_lock = threading.Lock()
work_added = threading.Condition()
init = False
work_queue = None
active_apps = None
monitor_t = None # monitor thread

def ensure_init():
    global structure_lock, active_apps, monitor_t, init, work_queue
    structure_lock.acquire()
    if not init:
        logging.debug("Initializing Local app task queue")
        active_apps = set()
        work_queue = Queue.Queue()
        monitor_t = MonitorThread(work_queue, active_apps)
        init = True
        monitor_t.start()
    
    structure_lock.release()

class MonitorThread(threading.Thread):
    def __init__(self, queue, active_apps):
        threading.Thread.__init__(self)
        self.queue = queue
        self.active_apps = active_apps
        self.setDaemon(True) # Ensure threads will exit with application
    
    def run(self):
        while True:
            # If there are no apps running, we just need to wait for something to
            # be added
            if len(self.active_apps) == 0:
                t = self.queue.get()
            else:
                # Check for new tasks - don't block because we need to monitor 
                # existing tasks
                if len(self.active_apps) >= MAX_RUNNING:
                    t = None
                else:
                    try:
                        t = self.queue.get_nowait()
                    except Queue.Empty:
                        t = None

            while t:
                #launch task  
                if t.run():
                    self.active_apps.add(t)

                if len(self.active_apps) >= MAX_RUNNING:
                    t = None
                else:
                    try:
                        t = self.queue.get_nowait()
                    except Queue.Empty:
                        t = None


            # Check to see if any existing tasks have finished, if so need to
            # invoke callback
            for app in self.active_apps.copy():
                if app.is_done():
                    logging.debug("App done!")
                    self.active_apps.remove(app)
                    self.queue.task_done()
                    # callback: TODO: should I have a separate thread for these?
                    app.do_callback()

            if len(self.active_apps) >= MAX_RUNNING:
                t = None
                time.sleep(POLL_INTERVAL)
            else:
                # Wait either the POLL_INTERVAL, or until there is work added to the queue
                global work_added
                work_added.acquire()
                if self.queue.empty():
                    work_added.wait(POLL_INTERVAL)
                work_added.release()


def openFile(fp, mode):
    if fp:
        return open(fp, mode)
    else:
        return None

class AppQueueEntry(object):
    def __init__(self, task, continuation, failure_continuation, contstack):
        """
        Std*_file should all be file paths
        """
        self.continuation = continuation
        self.failure_continuation = failure_continuation
        self.contstack = contstack
        self.task = task
        self.process = None
        self.exit_code = None

    def run(self):
        #TODO: handle errors here!!
        try: 
            task = self.task
    
            cmd_args, stdin_file, stdout_file, stderr_file = task._prepare_command()
            logging.info("Running app task %s.  Command line: %s." % (self.task.name(), 
                                    ' '.join(cmd_args)))
            if stdin_file is not None:
                logging.info("stdin redirected from %s" % stdin_file)
            if stdout_file is not None:
                logging.info("stdout redirected to %s" % stdout_file)
            if stderr_file is not None:
                logging.info("stderr redirected to %s" % stderr_file)
                
            if self.process is not None:
                raise Exception("Tried to run AppQueueEntry twice")
           
            stdin = stdout = stderr = None
            try:
                # Open the file if the path is not None
                stdin=openFile(stdin_file, 'r')
                stdout=openFile(stdout_file, 'w')
                stderr=openFile(stderr_file, 'w')
        
                logging.debug("Launching %s" % repr(cmd_args))
                #print "Starting a task"
                # launch the process
                
                exc = paths.lookup(cmd_args[0])
                if exc is not None:
                    # Use the binary we just looked up, otherwise
                    # use the OS's resolution mechanism
                    cmd_args[0] = exc
                try:
                    self.process = subprocess.Popen(cmd_args,
                        stdin = stdin, stdout = stdout,  
                        stderr = stderr,
                        close_fds=True # don't inherit any open files this
                                       # process may have. TODO: might not
                                       # work on windows
                        )
                except OSError, e:
                    self.failure_continuation(self.task, AppLaunchException(repr(self.task),                       
                                        cmd_args[0], e))
                    return False
            finally:
                # Don't need just-opened file descriptors in this process now
                for f in (stdin, stderr, stdout):
                    if f is not None:
                        f.close()
            self.task.started_callback()
            return True
        except Exception, e:
            self.failure_continuation(self.task, e)
            return False

    def is_done(self):
        if self.exit_code:
            return True
        self.exit_code = self.process.poll() # will be none if not finished
        return (self.exit_code is not None)
    
    def do_callback(self):
        if self.process is None:
            #TODO: how to handle
            raise Exception("Process has not yet been run, can't callback")
        #handle_cont = (self.contstack is not None and len(self.contstack) > 0  
        #        and self.contstack[0].__class__.__name__ == "AppTask") #TODO: temporary hack
        
        if self.exit_code != 0:
            # Error code
            self.failure_continuation(self.task, ExitCodeException(repr(self.task), self.exit_code))
            return
        
        handle_cont = False
        with graph_mutex:
            if len(self.task._outputs) == 1:
                retval = self.task._outputs[0]._bound
            else: 
                retval = [ch._bound for ch in self.task._outputs]
        if handle_cont:
            self.continuation(self.task, retval, None) # Handle own contstack
        else:
            self.continuation(self.task, retval, self.contstack) # Handle own contstack
        if handle_cont:
            launch_app(self.contstack[0], self.continuation, self.contstack[1:])


def launch_app(task, continuation, failure_continuation, contstack):
    """
    Launch a process using Popen, with cmd_args
    
    callback is a function that accepts a subprocess.Popen object as
    its single arg.  It will be called when the executable finishes running
    and returns a return code.

    output is redirected to/from the file paths specified by the 
    options std*_file arguments.
    """
    ensure_init()
    logging.debug("Added app %s to work queue" % repr(task))
    entry = AppQueueEntry(task, continuation, failure_continuation, contstack)
    work_added.acquire()
    work_queue.put(entry)
    work_added.notify()
    work_added.release()
