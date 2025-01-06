class Point:
  def __init__(self, x=0.0, y=0.0):
    if (isinstance(x, float) and isinstance(y,float)):
      self.x = x
      self.y = y
    else:
      print("error floating point is needed")
  def 
  def __str__(self):
    return f'Point {self.x}, {self.y}'

class LinkedList:
  class Node:
    def __init__(self, value, nextNode=None):
      self.value = value
      self.next = nextNode

  def __init__(self, *args):
    size = len(args)
    print('size', size)
    if size > 0:
      self.head = LinkedList.Node(args[0])
      size -= 1

      runner = self.head
      i = 1
      while i <= size:
        print(args[i])
        runner.next = LinkedList.Node(args[i])
        runner = runner.next
        i += 1
    else:
      self.head = None
  
  def push(self, val):
    runner = self.head

    while runner.next != None:
      runner = runner.next

    runner.next = LinkedList.Node(val)

  def pop(self):
    self.head = self.head.next

  def len(self):
    i = 0
    runner = self.head

    while runner != None:
      runner = runner.next
      i += 1

    return i

  def is_empty(self):
    return self.head == None

  
  def __str__(self):
    output = f""

    runner = self.head

    while runner != None:
      output += f'{runner.value} -> '
      runner = runner.next

    output += 'None'

    return output

from datetime import datetime
from time import sleep

class Machine:
  prices = (2,5)

  def __init__(self, type_machine):
    self.type_machine = type_machine
    self.start_time = None
    self.end_time = None
    self.total_time = 0

  def is_running(self):
    return self.start_time != None and self.end_time != None
  
  def start(self):
    if not(self.is_running()):
      self.start_time = datetime.now()
  
  def stop(self):
    if self.is_running():
      self.end_time = datetime.now()
      self.total_time += self.end_time - self.start_time
    
  def __repr__(self):
    return self.get_price()

  def __add__(self, o):
        return self.get_price() + o.get_price()

  def get_price(self):
    return int(self.total_time * Machine.prices[self.type_machine])
  
def main_machine():
  m1 = Machine(0)
  m2 = Machine(0)
  m3 = Machine(0)
  m4 = Machine(1)

  all_machines = [m1,m2,m3,m4] 

  m1.start()
  m2.start()
  m3.start()
  m4.start()

  sleep(1)
  m5 = Machine(1)
  all_machines.append(m5)

  sleep(1)
  m2.stop()

  sleep(1)
  for m in all_machines:
    m.stop()

  total_price = 0
  for m in all_machines:
    total_price += m.get_price()
  print(total_price)  




if __name__ == '__main__':
  p = Point()
  print(p)
  p2 = Point(1)
  #print(p2)
  p3 = Point(1, '4')

  l = LinkedList(1,2,3,4,5,6)
  print(l)
  l.push(7)
  print(l)
  print(l.len())
  print(l.is_empty())
  print(p3.__dict__)
  #main_machine()



