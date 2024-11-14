import tempfile
import psutil
import time
import ezmp
import os

def test_sleep():
	#
	print("just a sleep test")
	#

	@ezmp.backgrounded
	def bgtest():
		time.sleep(10000)

	sleep_pid = bgtest()
	os.kill(sleep_pid, 0)
	os.kill(sleep_pid, 9)

	sleep_pid = bgtest()
	ezmp.cleanup()

def test_background_loop():
	#
	print("checking that looping works")
	#

	tmp = tempfile.mktemp()

	@ezmp.backgrounded
	@ezmp.loop
	def bgtest2():
		open(tmp, "w").close()

	bgtest2()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert os.path.exists(tmp)

def test_suppression():
	#
	print("checking that exception suppressing works")
	#

	tmp = tempfile.mktemp()

	@ezmp.backgrounded
	@ezmp.loop
	def bgtest3():
		open(tmp, "w").close()
		raise Exception() #pylint:disable=broad-exception-raised

	bgtest3()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert not os.path.exists(tmp)

	@ezmp.backgrounded
	@ezmp.loop
	@ezmp.suppress(Exception)
	def bgtest4():
		open(tmp, "w").close()
		raise Exception() #pylint:disable=broad-exception-raised

	bgtest4()
	time.sleep(0.5)
	assert os.path.exists(tmp)
	os.unlink(tmp)
	time.sleep(0.5)
	assert os.path.exists(tmp)

def test_context_manager():
	#
	print("checking the context manager")
	#

	x = 1
	with ezmp.Task(run_parent=True, workers=0):
		x = 2
	assert x == 2

	x = 1
	with ezmp.Task(workers=0):
		x = 2
	assert x == 1

	start = time.time()
	with ezmp.Task(workers=1, wait=True) as bg:
		time.sleep(1)
	end = time.time()
	assert end - start > 1

	start = time.time()
	with ezmp.Task(workers=3, wait=True) as bg:
		time.sleep(bg.worker_id)
	end = time.time()
	assert end - start > 2

	start = time.time()
	with ezmp.Task(workers=3, timeout=1) as bg:
		time.sleep(bg.worker_id)
	end = time.time()
	assert end - start < 3

def test_stress():
	print("stress testing")
	with ezmp.Task(workers=ezmp.MAX_WORKERS) as t:
		time.sleep(100)
	t.terminate()
	assert not psutil.Process(os.getpid()).children()

def test_noop():
	a = 1
	with ezmp.Task(noop=True):
		a = 2
	assert a == 2

if __name__ == '__main__':
	test_sleep()
	test_context_manager()
	test_stress()
	test_noop()
