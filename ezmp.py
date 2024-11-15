import multiprocessing
import contextlib
import traceback
import logging
import signal
import atexit
import psutil
import time
import sys
import os
import io

parent_pid = os.getpid()

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.DEBUG)

def backgrounded(func):
    def _backgrounded(): #pylint:disable=inconsistent-return-statements
        with Task() as t:
            func()
        return t
    return _backgrounded

def loop(func):
    def loop_inner():
        while True:
            func()
    return loop_inner

def suppress(exc=None):
    def suppress_inner(func):
        def suppress_inner_inner():
            with contextlib.suppress(exc):
                func()
        return suppress_inner_inner
    return suppress_inner

def cleanup():
    if os.getpid() != parent_pid:
        return

    for t in active_tasks:
        if t.worker_pids:
            t.terminate()

def worker_term(sig, frame):
    raise EZMPTerm()
def raise_timeout(sig, frame):
    raise TimeoutError()

# from https://stackoverflow.com/questions/12594148/skipping-execution-of-with-block
class EZMPSkip(Exception):
    pass
class EZMPTerm(Exception):
    pass

active_tasks = [ ]
MAX_WORKERS = multiprocessing.cpu_count()

def await_availability(requested=1):
    while ( available := MAX_WORKERS - sum(len(t.worker_pids) for t in active_tasks) ) < requested:
        _LOG.debug("Waiting for availability: %s/%s", available, requested)
        wait_one()

def wait_one():
    c,_ = os.wait()
    for t in active_tasks:
        if c in t.worker_pids:
            t.worker_pids.remove(c)

def wait():
    for t in active_tasks:
        t.wait()

class Task():
    def __init__(
        self,
        noop=False, run_parent=False, wait=False, workers=1,
        timeout=None, buffer_output=False, atexit=None, silence_successes=False
    ): #pylint:disable=redefined-outer-name
        """
        Conditionally runs inner code.

        :param noop: Literally pretend that ezmp does not exist.
        :param run_parent: Run the code as the parent as well (default False)
        :param workers: Number of workers (default 1)
        :param wait: Wait for completion (default False)
        :param timeout: Timeout before workers are terminated (default None)
        :param buffer_output: Buffer stdout and print it out when the worker exits.
        :param atexit: Run this function as the task exits.
        :param silence_successes: When using buffered output, only emit tasks that throw exceptions.

        The timeout argument is mutually exclusive with wait and run_parent.
        """
        self._run_parent = run_parent
        self._wait = wait
        self.num_workers = workers
        self.timeout = timeout

        assert not (self.timeout and self._run_parent), "The timeout and run_parent arguments are mutually exclusive."
        assert not (self.timeout and self._wait), "The timeout and wait arguments are mutually exclusive."

        self.worker_pids = [ ]
        self.is_parent = None
        self.is_child = None
        self.worker_id = -1
        self.worker_pid = None
        self.buffer_output = buffer_output
        self.silence_successes = silence_successes
        self.noop = noop
        self.atexit = atexit

        if not noop:
            active_tasks.append(self)

    def __enter__(self):
        if self.noop:
            return self

        assert self.is_parent is not False

        self.is_parent = True
        for i in range(self.num_workers):
            try:
                await_availability(1)
            except KeyboardInterrupt:
                self.terminate()
                raise
            _LOG.debug("Task %s starting worker.", self)
            pid = os.fork()
            if pid:
                self.worker_pids.append(pid)
            else:
                self.is_parent = False
                self.worker_id = i
                self.worker_pid = os.getpid()
                break

        if not self.is_parent:
            self.worker_pids.clear()
            signal.signal(signal.SIGUSR1, self.worker_finish)
            signal.signal(signal.SIGTERM, self.worker_finish)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            if self.buffer_output:
                sys.stdout = io.StringIO()
                sys.stderr = sys.stdout

        if self.is_parent and not self._run_parent:
            sys.settrace(lambda *args, **keys: None)
            frame = sys._getframe(1)
            frame.f_trace = self.trace
        return self

    def trace(self, frame, event, arg):
        raise EZMPSkip()

    def __exit__(self, exc_type, value, tb): #pylint:disable=inconsistent-return-statements,redefined-builtin
        if self.noop:
            return

        if not self.is_parent:
            try:
                signal.signal(signal.SIGUSR1, signal.SIG_IGN)
                signal.signal(signal.SIGTERM, signal.SIG_IGN)

                if exc_type not in (None, EZMPTerm, EZMPSkip):
                    traceback.print_exception(exc_type, value, tb)
            finally:
                self.worker_finish(exc_type=exc_type, exc_value=value, exc_tb=tb)

        if self.is_parent and self.timeout:
            try:
                time.sleep(self.timeout)
                _LOG.debug("Timeout reached. Terminating workers.")
            except Exception: #pylint:disable=broad-exception-caught
                traceback.print_exc()
                _LOG.debug("Exception received. Terminating workers.")

            self.terminate()

        if self.is_parent and self._wait:
            _LOG.debug("Waiting for workers.")
            try:
                self.wait()
            except: #pylint:disable=bare-except
                traceback.print_exc()
                _LOG.debug("Exception received. Terminating workers.")
                self.terminate()

        if exc_type is None:
            return
        if issubclass(exc_type, EZMPSkip):
            return True

    def worker_finish(self, sig=None, frame=None, exc_type=None, exc_value=None, exc_tb=None): #pylint:disable=unused-argument
        try:
            if self.atexit:
                self.atexit()
            if self.buffer_output and not (self.silence_successes and exc_type is None):
                print(sys.stdout.getvalue(), end="", file=sys.__stdout__)
            _LOG.debug("Worker ID %d PID %d terminating.", self.worker_id, self.worker_pid)
        except: #pylint:disable=bare-except
            traceback.print_exc()
        finally:
            os.kill(self.worker_pid, 9)

    def terminate(self):
        try:
            _LOG.debug("Terminating %s...", self)
            print(f"Terminating task {self}. One more SIGINT to force-kill.", file=sys.stderr)
            for c in self.worker_pids:
                _LOG.debug("... terminating %s child PID %s", self, c)
                with contextlib.suppress(ProcessLookupError):
                    os.kill(c, signal.SIGUSR1)

            self.wait(timeout=10)
        except KeyboardInterrupt:
            print(f"KeyboardInterrupt: force-killing workers of task {self}...", file=sys.stderr)
            for c in self.worker_pids:
                with contextlib.suppress(ProcessLookupError):
                    child = psutil.Process(c)
                    for descendent in child.children(recursive=True):
                        descendent.kill()
                    child.kill()
            raise

        if self.worker_pids:
            self.terminate()

    def wait(self, timeout=None):
        if timeout:
            signal.signal(signal.SIGALRM, raise_timeout)
            signal.alarm(timeout)

        try:
            while self.worker_pids:
                wait_one()
        except TimeoutError:
            _LOG.debug("Timeout waiting for children.")

        if timeout:
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            signal.alarm(0)

atexit.register(cleanup)
