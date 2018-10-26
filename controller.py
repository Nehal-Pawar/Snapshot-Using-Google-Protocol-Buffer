# -*- coding: utf-8 -*-
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2

import socket
fname='testfile.txt'
#file = open(fname, 'r') 
#branch= file.read() 

with open(fname) as f:
    for each in f:  
    
      print each
      ip= each.split()[1]      
      port=each.split()[2]      
      inport=int(port.strip('\0'))
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      BR=bank_pb2.BranchMessage()
      branch=BR.init_branch
      branch.balance=100
      s.connect((ip,int(inport)))
      s.send(BR.SerializeToString())
      print s.recv(1024)