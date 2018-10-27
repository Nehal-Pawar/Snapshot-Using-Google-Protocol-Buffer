# -*- coding: utf-8 -*-
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2

import socket
fname='testfile.txt'
#file = open(fname, 'r') 
#branch= file.read() 
count=0
BR=bank_pb2.BranchMessage()


with open(fname) as f:
    for each in f:  
      
      print each
      ip= each.split()[1]      
      port=each.split()[2]      
      inport=int(port.strip('\0'))      
      bank=BR.init_branch
      
      branch1=bank.all_branches.add()
      #BR=bank_pb2.BranchMessage.add()
      branch1.name=each.split()[0]
      branch1.ip=ip
      branch1.port=inport
      count=count+1

f.close()

total_balance = int(sys.argv[1])
bank.balance = total_balance / count

with open(fname) as f:
    for each in f:  
      ip= each.split()[1]      
      port=each.split()[2]
      inport=int(port.strip('\0'))
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
      #print ip,inport
      s.connect((ip,int(inport)))
      s.send(BR.SerializeToString())
      print s.recv(1024)
      
      s.close()
      
f.close()