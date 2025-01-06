def remove_non_str(list):
  """
  1. Create a function that receives a list and removes all the non str objects from it.
  """
  return [x for x in list if isinstance(x, str)]

def string2dict(string):
  """
  2.Create a function that receives a str and returns a dict containing each letter 
  and its occurrence.

  Now do it without using if
  """
  return {(char, string.count(char)) for char in string}

def intersection_lists(list1, list2):
  """
  3.Create a function that receives 2 lists and returns a list containing only 
  the elements that are in both lists.    
  """
  return list(set(list1) & set(list2))
  
def unique_values(dic):
  """
  4.Write a function that receives a dictionary and returns list with 
  all unique values in a given dictionary.
  """

  return list(set(dic.values()))

def rotate_left(liste):
  """
  5.Create a function that receives a list and performs a left rotation using slicing.
  """
  return liste[1:] + [liste[0]]

def six():
  """
  6.Write a function that removes and prints every second number from a 
  list of numbers until the list becomes empty.
  """
  pass


def seven():
  """
  7.Write a function to convert a given dictionary to a list of tuples.

  """
  pass


def eight(dic):
  """
  8.Write a function to find the maximum and the minimum values in a dictionary
   and print their keys.

  """
  print(dic)

  dic = dict(sorted(dic.items(), key=lambda k: k[1]))
  dic
  print(dic)


def test1():
  assert remove_non_str([1, 2, 3, "a", "b", "c"]) == ["a", "b", "c"]

def test2():
  assert string2dict("abcaA") == {("a", 2), ("b", 1), ("A", 1), ("c", 1)}

def test3():
  l = intersection_lists([1, 2, 3], [2, 3, 4])
  l.sort()
  assert l == [2, 3]

def test4():
  d = dict()
  d["1"] = 1
  d["2"] = 1
  d["3"] = 5
  
  l = unique_values(d)
  l.sort()
  assert l == [1,5]
  
def test5():
  assert rotate_left([1,2,3,4]) == [2,3,4,1]
  
def test6():

  pass



def test7():
  pass
def test8():
  d = dict()
  d[1]=100
  d[0]= 85

  eight(d)




if __name__ == "__main__":
  #test1()
  #test2()
  #test3()
  #test4()
  #test5()
  test6()
  #test7()
  #test8()


  print("All tests passed")

