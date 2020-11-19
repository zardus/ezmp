import contextlib
import atexit
import os

parent_pid = os.getpid()
children = set()

def background(func):
	def backgrounded():
		pid = os.fork()
		if pid:
			children.add(pid)
			return pid
		func()
		os.kill(os.getpid(), 9)
	return backgrounded

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
		os.kill(c, 9)
		os.waitpid(c, 0)
	children.clear()

atexit.register(cleanup)

if __name__ == '__main__':
	import tempfile
	import time

	#
	# just a sleep test
	#

	@background
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

	@background
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

	@background
	@loop
	def bgtest2():
		open(tmp, "w").close()
		raise Exception()

	bgtest2()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert not os.path.exists(tmp)

	@background
	@loop
	@suppress(Exception)
	def bgtest3():
		open(tmp, "w").close()
		raise Exception()

	bgtest3()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert os.path.exists(tmp)

	print("SUCCESS")
