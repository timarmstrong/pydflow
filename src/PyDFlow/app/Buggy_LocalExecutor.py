from __future__ import with_statement
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
import os
import threading
import Queue
import logging
import paths

#TODO: no polling?
POLL_INTERVAL = 0.05 # seconds between polling tasks when active
#TODO: get info about number of processors
MAX_RUNNING = 10

LIFO = False

structure_lock = threading.Lock()
init = False
work_queue = None
active_apps = None
monitor_t = None # monitor thread

def ensure_init():
    global structure_lock, active_apps, monitor_t, init, work_queue
    structure_lock.acquire()
    if not init:
        logging.debug("Initializing Local app task queue")
        work_queue = Queue.Queue()
        notify_queue = Queue.Queue()
        running_semaphore = threading.Semaphore(MAX_RUNNING)
        executor_t = threading.Thread(target=accept_work, args=(work_queue, notify_queue, running_semaphore))
        monitor_t = threading.Thread(target=monitor_processes, args=(notify_queue, running_semaphore))
        init = True
        executor_t.start()
        monitor_t.start()
    
    structure_lock.release()


def accept_work(work_queue, notify_queue, semaphore):
    while True:
        # If there are no apps running, we just need to wait for something to
        # be added
        semaphore.acquire()
        app = work_queue.get()
        work_queue.task_done()
        app.run()
        notify_queue.put(app)
        logging.debug("Got work %s" % repr(app))
                    
def monitor_processes(notify_queue, semaphore):
    active_apps = {}
    while True:
        # First wait until something finishes\
        
        #sif len(active_apps) > 0:
            #logging.debug("os,wait()")
            #pid, stat = os.wait()
            #logging.debug("Pid %d finished" % pid)
            #block = True
        #else:
            #block = False
#            pid = None
        
        # Make sure active_apps dict is up to date
        queue_notempty = True
        while queue_notempty:
            try:
                app = notify_queue.get(False, POLL_INTERVAL)
                notify_queue.task_done()
                logging.debug("Monitor app %s %d" % (repr(app), app.process.pid))
                active_apps[app.process.pid] = app
            except Queue.Empty:
                queue_notempty = False
        #if pid is not None:
        #    match = active_apps.get(pid, None)
        for app in active_apps.values():
            if app.is_done():
                logging.debug("App done!")
                del active_apps[app.process.pid]
                semaphore.release()
                # Do callbacks in separate thread
                callback_thread = threading.Thread(target=app.do_callback)
                callback_thread.start()
        


class AppQueueEntry(object):
    def __init__(self, task, continuation):
        """
        Std*_file should all be file paths
        """
        self.continuation = continuation
        self.task = task
        self.process = None
        self.exit_code = None

    def run(self):
        task = self.task

        cmd_args, stdin_file, stdout_file, stderr_file = task._prepare_command()
        if self.process is not None:
            raise Exception("Tried to run AppQueueEntry twice")
       
        # Open the file if the path is not None
        openFile = lambda fp, mode: fp and open(fp, mode)
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
        self.process = subprocess.Popen(cmd_args,
            stdin = stdin, stdout = stdout,  
            stderr = stderr,
            close_fds=True # don't inherit any open files this
                           # process may have. TODO: might not
                           # work on windows
            )
        # Don't need just-opened file descriptors in this process now
        for f in (stdin, stderr, stdout):
            if f is not None:
                f.close()
        self.task.started_callback()

    def is_done(self):
        if self.exit_code:
            return True
        self.exit_code = self.process.poll() # will be none if not finished
        return (self.exit_code is not None)
    
    def do_callback(self):
        #logging.debug("do_callback")
        if self.process is None:
            raise Exception("Process has not yet been run, can't callback")
        with graph_mutex:
            if len(self.task._outputs) == 1:
                retval = self.task._outputs[0]._bound
            else: 
                retval = [ch._bound for ch in self.task._outputs]
            self.continuation(self.task, retval)


def launch_app(task, continuation):
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
    entry = AppQueueEntry(task, continuation)
    work_queue.put(entry)
