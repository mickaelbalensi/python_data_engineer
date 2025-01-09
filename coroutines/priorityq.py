import heapq
class priorityq:
  def __init__(self):
    self.q = []
    self.index = 0
    self.type_priority = None

  def is_empty(self):
    return len(self.q) == 0
    
  def __len__(self):
    return len(self.q)

  
  def push(self, priority, item):
    if self.type_priority is None:
      self.type_priority = type(priority)
    elif not isinstance(priority, self.type_priority):
      raise TypeError(f"Priority must be an {self.type_priority} and not {type(priority)}")

    heapq.heappush(self.q, (priority, self.index, item))
    self.index += 1

  def pop(self):
    if self.is_empty():
      raise IndexError("Priority queue is empty")
    
    priority, _, item = heapq.heappop(self.q)
    return priority, item
  
  def peek(self):
    if self.is_empty():
      raise IndexError("Priority queue is empty")
    return self.q[0][0], self.q[0][2]