import subprocess
import threading
import Queue
import logging

#TODO: no polling?
POLL_INTERVAL = 0.1 # seconds between polling tasks when active
#TODO: get info about number of processors
MAX_RUNNING = 4

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
        active_apps = set()
        if LIFO:
            work_queue = Queue.LifoQueue()
        else:
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
                t.run()
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

            # Wait either the POLL_INTERVAL, or until there is work added to the queue
            global work_added
            work_added.acquire()
            if self.queue.empty():
                work_added.wait(POLL_INTERVAL)
            work_added.release()


class AppQueueEntry(object):
    def __init__(self, task, cmd_args, stdout_file, stdin_file, stderr_file):
        """
        Std*_file should all be file paths
        """
        self.task = task
        self.cmd_args = cmd_args
        self.stdout_file = stdout_file
        self.stdin_file = stdin_file
        self.stderr_file = stderr_file
        self.process = None
        self.exit_code = None

    def run(self):
        if self.process is not None:
            raise Exception("Tried to run AppQueueEntry twice")
       
        # Open the file if the path is not None
        openFile = lambda fp, mode: fp and open(fp, mode)
        self.stdin=openFile(self.stdin_file, 'r')
        self.stdout=openFile(self.stdout_file, 'w')
        self.stderr=openFile(self.stderr_file, 'w')

        logging.debug("Launching %s" % repr(self.cmd_args))
        # launch the process
        self.process = subprocess.Popen(self.cmd_args,
            stdin = self.stdin, stdout = self.stdout,  
            stderr = self.stderr,
            close_fds=True # don't inherit any open files this
                           # process may have. TODO: might not
                           # work on windows
            )
        # Don't need just-opened file descriptors in this process now
        for f in [self.stdin, self.stderr, self.stdout]:
            if f is not None:
                f.close()
        self.task.started_callback()

    def is_done(self):
        if self.exit_code:
            return True
        self.exit_code = self.process.poll() # will be none if not finished
        return (self.exit_code is not None)
    
    def do_callback(self):
        if self.process is None:
            raise Exception("Process has not yet been run, can't callback")
        self.task.finished_callback(self.process)


def launch_app(task, cmd_args, stdin_file=None, stdout_file=None, stderr_file=None):
    """
    Launch a process using Popen, with cmd_args
    
    callback is a function that accepts a subprocess.Popen object as
    its single arg.  It will be called when the executable finishes running
    and returns a return code.

    output is redirected to/from the file paths specified by the 
    options std*_file arguments.
    """
    ensure_init()

    entry = AppQueueEntry(task, cmd_args, stdout_file, stdin_file, stderr_file)
    work_added.acquire()
    work_queue.put(entry)
    work_added.notify()
    work_added.release()
