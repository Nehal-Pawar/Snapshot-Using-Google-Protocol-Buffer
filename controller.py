# -*- coding: utf-8 -*-

import sys
import socket
fname='testfile.txt'
#file = open(fname, 'r') 
#branch= file.read() 

with open(fname) as f:
    branch= f.readlines()  
for each in branch:  
    if each:
      print each
      ip= each.split()[1]
      port=each.split()[2]
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      
      s.connect((ip,int(port)))
      s.send("message send from controler")