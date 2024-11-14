import contextlib
import traceback
import logging
import signal
import atexit
import psutil
import time
import sys
import gc
import os
import io

parent_pid = os.getpid()
children = set()

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.DEBUG)

def backgrounded(func):
	def _backgrounded(): #pylint:disable=inconsistent-return-statements
		pid = os.fork()
		if pid:
			children.add(pid)
			return pid
		func()
		os.kill(os.getpid(), 9)
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

	for c in children:
		with contextlib.suppress(ProcessLookupError):
			os.kill(c, 9)
			os.waitpid(c, 0)
	children.clear()

def worker_term(sig, frame):
	raise EZMPTerm()

# from https://stackoverflow.com/questions/12594148/skipping-execution-of-with-block
class EZMPSkip(Exception):
	pass
class EZMPTerm(Exception):
	pass


class background_ctx():
	def __init__(self, run_parent=False, wait=False, workers=1, timeout=None, buffer_output=False):
		"""
		Conditionally runs inner code.

		:param run_parent: Run the code as the parent as well (default False)
		:param workers: Number of workers (default 1)
		:param wait: Wait for completion (default False)
		:param timeout: Timeout before workers are terminated (default None)
		:param buffer_output: Buffer stdout and print it out when the worker exits.

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

	def __enter__(self):
		self.is_parent = True
		for i in range(self.num_workers):
			pid = os.fork()
			if pid:
				self.worker_pids.append(pid)
				children.add(pid)
			else:
				self.is_parent = False
				self.worker_id = i
				self.worker_pid = os.getpid()
				break

		if not self.is_parent:
			self.worker_pids.clear()
			signal.signal(signal.SIGTERM, worker_term)
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
		if not self.is_parent:
			if exc_type not in (None, EZMPTerm, EZMPSkip):
				traceback.print_exception(exc_type, value, tb)
			if exc_type:
				_LOG.debug("Worker ID %d PID %d got exception type %s", self.worker_id, self.worker_pid, exc_type)
				# deallocate stuff hanging on the exception context
				del exc_type
				gc.collect()

			if self.buffer_output:
				print(sys.stdout.getvalue(), end="", file=sys.__stdout__)
			_LOG.debug("Worker ID %d PID %d terminating.", self.worker_id, self.worker_pid)
			os.kill(self.worker_pid, 9)

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

	def terminate(self):
		# send all the kill signals
		for c in self.worker_pids:
			with contextlib.suppress(ProcessLookupError):
				os.kill(c, signal.SIGTERM)

		# let's get nasty after a second
		time.sleep(1)
		for c in self.worker_pids:
			with contextlib.suppress(ProcessLookupError):
				child = psutil.Process(c)
				for descendent in child.children(recursive=True):
					descendent.kill()
				os.kill(c, signal.SIGTERM)

		self.wait()

	def wait(self):
		for c in self.worker_pids:
			with contextlib.suppress(ProcessLookupError):
				os.waitpid(c, 0)
		self.worker_pids = [ ]


atexit.register(cleanup)

if __name__ == '__main__':
	import tempfile

	#
	# just a sleep test
	#

	@backgrounded
	def bgtest():
		time.sleep(10000)

	sleep_pid = bgtest()
	os.kill(sleep_pid, 0)
	os.kill(sleep_pid, 9)

	sleep_pid = bgtest()
	cleanup()

	#
	# check that looping works
	#

	tmp = tempfile.mktemp()

	@backgrounded
	@loop
	def bgtest2():
		open(tmp, "w").close()

	bgtest2()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert os.path.exists(tmp)

	#
	# check that exception suppressing works
	#

	tmp = tempfile.mktemp()

	@backgrounded
	@loop
	def bgtest3():
		open(tmp, "w").close()
		raise Exception() #pylint:disable=broad-exception-raised

	bgtest3()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert not os.path.exists(tmp)

	@backgrounded
	@loop
	@suppress(Exception)
	def bgtest4():
		open(tmp, "w").close()
		raise Exception() #pylint:disable=broad-exception-raised

	bgtest4()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert os.path.exists(tmp)

	#
	# check the context manager
	#

	x = 1
	with background_ctx(run_parent=True, workers=0):
		x = 2
	assert x == 2

	x = 1
	with background_ctx(workers=0):
		x = 2
	assert x == 1

	start = time.time()
	with background_ctx(workers=1, wait=True) as bg:
		time.sleep(1)
	end = time.time()
	assert end - start > 1

	start = time.time()
	with background_ctx(workers=3, wait=True) as bg:
		time.sleep(bg.worker_id)
	end = time.time()
	assert end - start > 2

	start = time.time()
	with background_ctx(workers=3, timeout=1) as bg:
		time.sleep(bg.worker_id)
	end = time.time()
	assert end - start < 2

	print("SUCCESS")
