# ezmp

Do you feel that the multiprocessing module is too complicated?
Futures got you down?
Multiprocessing keeps messing up?
Welcome to ezmp!

ezmp is designed to require no thought to use.
Want to just execute a bunch of functions in the background?
No problem!

```
import ezmp

@ezmp.backgrounded
def background_task():
    import time
    time.sleep(1000)

task = background_task()
print("Background task is running!")
task.wait()
```

Want to execute random code in parallel?
Sure thing!

```
for i in range(100):
    with ezmp.Task() as t:
        print(i)
ezmp.wait()
```

Want to execute code a bunch of times?
Sure thing!

```
with ezmp.Task(workers=100, wait=True) as t:
    print(f"Worker {c.worker_id} reporting!")
```

Want to debug your buggy multiprocessing code without the pain of multiprocessing?
You got it!

```
with ezmp.Task(noop=True):
    print("This runs 100% normally in the parent.")
```

Other useful decorators: `@loop`, `@suppress(Exception)`
