from priorityq import priorityq
from typing import Union, Callable, Generator
from collections.abc import Generator as GeneratorType

import time

class scheduler:
  def __init__(self):
    self.pq = priorityq()

  def add(self, timepoint: float, callback: Union[Callable, Generator], frequency: float = None):
      if not isinstance(callback, GeneratorType):
        def wrapp_function_to_generator():
          while True:
            need_reschedule = callback()
            if need_reschedule:
              yield frequency
            else:
              break
        task = wrapp_function_to_generator()  
      else:    
        task = callback
      self.pq.push(timepoint, task)

  def run(self):
    while not self.pq.is_empty():
      timepoint, task = self.pq.pop()
      next_freq = None 

      now = time.time()
      if now < timepoint:
        time.sleep(timepoint - now)

      try: 
        next_freq = next(task)
        
      except StopIteration as s:
        print(f"StopIteration: {s}")
        
      except Exception as e:
        print(f"Error executing task: {e}")
      else:
        try:
          if next_freq:
            next_timepoint = time.time() + next_freq
            self.pq.push(next_timepoint, task)
        except Exception as e:
          print(f"Error rescheduling task: {e}")wrapp_function_to_generator

def t1():
  print("t1")
  return True

def t2():
  print("\tt2")
  return True

def t3():
  print("\t\tt3")
  return False

def coroutine():
  for i in range(1,5):
    print(f'\t\t\tcoroutine: {i}') 
    yield i

print(type(coroutine))
# sched.add(time.time() + 1, coroutine(), 1)
# sched.add(time.time() + 2, t1, 5)
# sched.add(time.time() + 1, t2, 10)
# sched.add(time.time() + 6, t3)


sched.run()