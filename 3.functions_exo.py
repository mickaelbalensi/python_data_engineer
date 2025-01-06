def remove_words_from_list():
  """
  1.Write a program to remove specific words from a given list using lambda. 
  """

  words = ["apple", "banana", "cherry", "date", "fig", "grape"]
  words_to_remove = {"banana", "date", "fig"}

  list_updated = list(filter(lambda word: word not in words_to_remove, words))
  print(list_updated)

def sort_list_str():
  """
  2.Write a program to sort a list of strings(numbers) numerically. Use lambda.   
  """

  list_numbers = ["1", "2", "-5", "-9", "0"]
  print("original list", list_numbers)

  list_sorted = list(map(lambda num: str(num), sorted(list(map(lambda x: eval(x),\
                                                                list_numbers)))))
  print("sorted list", list_sorted)

def square_list():
  """
  4.Using list comprehension, construct a list from the squares of each even 
  element in the given list.
  """
  list_num = [1,2,3,4,5,6]
  squares = [x**2 for x in list_num if x % 2 == 0]

  print("list_num", list_num)
  print("squares", squares)

def sales_prices():
  """
  5.Write a function that receives the dictionary with names of products and 
  their prices and returns the dictionary with a 10% sale price.
  """
  def func(dict_prices):
    for k,v in dict_prices.items():
      dict_prices[k] = 0.9 * v

  d = dict()
  d[1] = 100
  d[2] = 200
  func(d)
  print(d)

def guematria():
  ''' 
  6.Gematria is the practice of assigning a numerical value to a name, word, or phrase according to an alphanumerical cipher. 
  write a program that returns the value of the Hebrew phrase/word. 
  You can use the Gematria table on the Wiki : https://en.wikipedia.org/wiki/Gematria

  Examples: 
  שלום" =  376" 
  323 = "אינפיניטי לאבס"
  '''

  gematria_table = {
    'א': 1, 'ב': 2, 'ג': 3, 'ד': 4, 'ה': 5, 'ו': 6, 'ז': 7, 'ח': 8, 'ט': 9,
    'י': 10, 'כ': 20, 'ל': 30, 'מ': 40, 'ם':40, 'נ': 50, 'ס': 60, 'ע': 70, 'פ': 80, 
    'צ': 90, 'ק': 100, 'ר': 200, 'ש': 300, 'ת': 400
  }

  def gematria_value(phrase):
      total_value = 0
      for char in phrase:
          if char in gematria_table: 
              total_value += gematria_table[char]

      return total_value

  
  phrase1 = "שלום"
  phrase2 = "אינפיניטי לאבס"

  print(f"Gematria value of '{phrase1}': {gematria_value(phrase1)}")
  print(f"Gematria value of '{phrase2}': {gematria_value(phrase2)}")

def luhn():
  '''
  7.Implement the Luhn algorithm for the validation of credit card numbers.    

  Description of algorithm:  
  The first step of the Luhn algorithm is to double every second digit, starting from the right.    
  If doubling the number results in a number greater than 9, subtract 9 from the product.     
  Then take the sum of all the digits.      
  If the sum is evenly divisible by 10, then the number is valid. 
    '''
  def luhn_algo(number):
    str_number = str(number)[::-1]

    list_digits = [int(num) if i % 2 == 0 else int(num) * 2 for i, num in enumerate(str_number)]

    list_digits = [num if i % 2 == 0 else num if num <= 9 else num - 9 for i, num in enumerate(list_digits)]

    summe = sum(list_digits)
    return summe % 10 == 0

  luhn_algo(123456789)


if __name__ == "__main__":
  remove_words_from_list()
  sort_list_str()
  square_list()
  sales_prices()
  #guematria()
  luhn()
