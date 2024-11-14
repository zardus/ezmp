# ezmp

Do you feel that the multiprocessing module is too complicated? Futures got you down? Welcome to ezmp!

```
@backgrounded
def background_task():
    import time
    time.sleep(1000)

task_pid = background_task()
print("Background task is running!")
os.waitpid(task_pid, 0)

with Task(workers=3) as c:
    print(f"Worker {c.worker_id} reporting!")
```

Other useful decorators: `@loop`, `@suppress(Exception)`
