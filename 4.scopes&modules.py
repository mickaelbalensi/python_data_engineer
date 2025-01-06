import areas7

import os
import sys
import stat
def use_mymodule():
  print(areas.circle_area(5))

def use_os_module():
  print(os.name )
  print(os.getlogin())
  print(os.getcwd())

def use_sys_module():
  argvs = sys.argv
  argvs.reverse()
  for a in argvs:
    print(a)

def change_permissions_file(filename):
  path = "."
  for root, dirs, files in os.walk(path):
    if filename in files:
      filepath = os.path.join(root, filename)
      file_stat = os.stat(filepath).st_mode
      print("existssssssssssssssss")

      print(oct(file_stat)[-3:])
      
      if file_stat & stat.S_IXUSR:
        print("owner already have  execut permissions")
      else:
        print("change mode")
        os.chmod(filepath, file_stat | stat.S_IXUSR )

      if file_stat & stat.S_IXGRP:
        print("GRP already have  execut permissions")
      else:
        print("change mode")
        os.chmod(filepath, file_stat | stat.S_IXGRP )

      os.chmod(filepath, file_stat & ~stat.S_IXOTH )

#use_mymodule()
#use_os_module() 
#use_sys_module()
change_permissions_file("areas.py")



