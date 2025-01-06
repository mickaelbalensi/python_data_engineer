import pytest

class BankAccount:
  def __init__(self, id):
    self.id = id
    self.balance = 0

  def withdraw(self, amount):
    if self.balance >= amount:
      self.balance -= amount
      return True
    return False

  def deposit(self, amount):
    self.balance += amount
    return True

def test_deposit():
  account = BankAccount(5)
  account.deposit(10)
  assert account.balance == 10
  print('deposit is tested')


def test_withdraw():
  account = BankAccount(5)

  assert account.withdraw(0) == True
  assert account.balance == 0

  assert account.withdraw(5) == False
  assert account.balance == 0

  account.deposit(10)
  assert account.withdraw(3) == True
  assert account.balance == 7

  print("test withdraw")

