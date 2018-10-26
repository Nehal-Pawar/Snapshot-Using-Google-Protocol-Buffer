# -*- coding: utf-8 -*-

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import addressbook_pb2
import sys
import socket
fname='testfile.txt'
#file = open(fname, 'r') 
#branch= file.read() 

with open(fname) as f:
    for each in f:  
    
      print each
      ip= each.split()[1]      
      port=each.split()[2]
      print port
      inport=int(port.strip('\0'))
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      #BR=BranchMessage()
      #branch=BR.init_branch.add()
      #branch.balance=100 
      s.connect((ip,int(inport)))
      s.send("message send from controler")
      print s.recv(1024)