from dataclasses import dataclass
from typing import Optional

@dataclass
class Node:
    value: int  # or whatever type you're using
    left: Optional['Node'] = None
    right: Optional['Node'] = None

# class Node:
#   def __init__(self, val):
#     self.left = None
#     self.right = None
#     self.value = val

class BST:
  def __init__(self):
    self.root = None

  def insert(self, val):
    if not self.root:
      self.root = Node(val)
      return

    slow = self.root
    fast = slow.left if val < slow.value else slow.right

    while fast:
      slow = fast
      fast = slow.left if val < slow.value else slow.right

    if val < slow.value:
      slow.left = Node(val)
    else:
      slow.right = Node(val)
 
  def __getitem__(self, index):
    if index < 0:
        raise IndexError("Index out of range")

    class Counter:
        def __init__(self):
            self.count = 0
            self.result = None
            
    def find_nth(node, n, counter):
      if not node or counter.result is not None:
        return    
          
      find_nth(node.left, n, counter)

      if counter.result is None and counter.count == n:
        counter.result = node.value
        return

      counter.count += 1
      find_nth(node.right, n, counter)
    
    counter = Counter()
    find_nth(self.root, index, counter)
    
    if counter.result is None:
        raise IndexError("Index out of range")
        
    return counter.result

  def is_empty(self):
    return not self.root 

  def __contains__(self, val):
    return self.find(val)

  def pre(self, root):
    if root == None:
      return  
    yield root.value
    yield from self.pre(root.left)
    yield from self.pre(root.right)

  def post(self, root):
    if root == None:
      return  
    yield from self.post(root.left)
    yield from self.post(root.right)
    yield root.value

  def inorder(self, root):
    if root == None:
      return  
    yield from self.inorder(root.left)
    yield root.value
    yield from self.inorder(root.right)

  # Removed code because, now, __getitem__ will generate __iter__ function

  # def __iter__(self):
  #   yield from self.inorder(self.root)

  def __len__(self):
    return sum(1 for _ in self)

  def find(self, val):
    runner = self.root
    
    while runner:
      if runner.value == val:
        return True

      runner = runner.left if runner.value > val else runner.right
    return False

  def remove(self, val):
    def find_min(node):
        current = node
        while current.left:
            current = current.left
        return current

    def _remove(root, val):
        if not root:
            return None
            
        # Find the node
        if val < root.value:
            root.left = _remove(root.left, val)
        elif val > root.value:
            root.right = _remove(root.right, val)
        else:
            # Node found! Handle the three cases:
            
            # Case 1 & 2: No child or one child
            if not root.left:
                return root.right
            elif not root.right:
                return root.left
                
            # Case 3: Two children
            # Find inorder successor (smallest in right subtree)
            temp = find_min(root.right)
            root.value = temp.value
            # Delete the successor
            root.right = _remove(root.right, temp.value)
            
        return root
        
    self.root = _remove(self.root, val)


tree = BST()
print(f'is empty ? {tree.is_empty()}')

tree.insert(5)
print(f'is empty ? {tree.is_empty()}')

tree.insert(3)
tree.insert(7)
tree.insert(1)
tree.insert(4)

for i in range(5):
  print(tree[i])

  #      5
  #     / \
  #    3   7
  #   / \
  #  1   4

# print(tree.root.value)
# print(tree.root.left.value)
# print(tree.root.right.value)
# print(tree.root.left.left.value)
# print(tree.root.left.right.value)

print("pre order traversal")
preorder = tree.pre(tree.root)
for val in preorder:
  print(val, end = ' ')
  
print("\npost order traversal")
for val in tree.post(tree.root):
  print(val, end = ' ')

print("\nin order traversal")
for val in tree.inorder(tree.root):
  print(val, end = ' ')

print("\niterable traversal")
for val in tree:
  print(val, end = ' ')

print("\nlen of tree")
print(len(tree))

print("find test")
for val in tree:
  assert tree.find(val) == True
  print("find ",val)

assert tree.find(9) == False

print("remove test")
tree.remove(7)
for val in tree:
  print(val, end = ' ')

