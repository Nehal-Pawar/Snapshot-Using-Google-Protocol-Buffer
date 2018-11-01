import socket
import os
import sys
import thread
import time

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')


import bank_pb2
import threading
class MethodsREQ:
  branchlist = []
  balance = 0
  
  def Transfer(self):
    transfer_amt = bank_pb2.BranchMessage()
    
    transfer =  transfer_amt.transfer
    transfer.src_branch = sys.argv[1]
    transfer.dst_branch = 'branch3'
    transfer.money = 50
    self.balance = self.balance - transfer.money
    self.branchlist[0].send("zxcv jo bhiui : " +sys.argv[1])
    self.branchlist[1].send("zxcv jo bhiui : " +sys.argv[1])
    self.branchlist[2].send("zxcv jo bhiui : " +sys.argv[1])
    #print 'this is running in tansfer msg methoid ' + str(transfer)
    
  def connect2(self,ip,port):
	 #thread.start_new_thread(transfer,(clientSocket, clientAddress))
	 s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	 #print ip,inport
	 s.connect((ip,int(port)))
	 s.send("received zxcv from : "+sys.argv[1])
	 self.branchlist.append(s)
	 #print s.recv(1024)'
  
  def Threading(self,clientSocket, clientAddress):
  	# Receive the message
  	msg = clientSocket.recv(1024)
  	print msg
  	bankdetails = bank_pb2.BranchMessage()
  	if "zxcv" in msg:
  		print msg
  		return
  	bankdetails.ParseFromString(msg)
  	
  	#print bankdetails
  
  	if bankdetails.HasField('init_branch'):
  		self.balance =  bankdetails.init_branch.balance
  		
  		print 'message is init branch and starting to connet'
  		for each_branch in bankdetails.init_branch.all_branches:
  			if each_branch.name != sys.argv[1]:
  				 #connlect_to_branches(each_branch)
  				 print each_branch.name
  				 threading.Thread(target=self.connect2,args=(each_branch.ip,each_branch.port)).start()
  				
      
  		time.sleep(2)     
  		if sys.argv[1]!= 'branch2':
  			self.Transfer()
	#print self.branchlist
  
  	if bankdetails.HasField('transfer'):
  		print bankdetails.transfer.src_branch
  		print bankdetails.transfer.dst_branch
  		print bankdetails.transfer.money
  
  		self.balance = self.balance + bankdetails.transfer.money
  
  		print 'balance ' + str(self.balance)
  	response = 'Message recieved to ' + sys.argv[1]
  	# Send the response to client
  	clientSocket.send(response)
  
  	# Close the client socket
  	clientSocket.close()
  
  
  

# No command line arguments needed
if len(sys.argv) != 4:
	print("Usage:", sys.argv[0], "Branch name", "Port number", "Maximum Interval in ms")
	sys.exit()

# Create a TCP Server Socket
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Binding socket on an unused port
serverSocket.bind(('',int(sys.argv[2])))

# Print host name and port number of server
host_name = socket.gethostbyname(socket.gethostname())
server_port = serverSocket.getsockname()[1]
print host_name + ' listening on port ' + str(server_port)

abc=MethodsREQ()
serverSocket.listen(1)

while 1:
	# Accepting the client request
	clientSocket, clientAddress = serverSocket.accept()
	thread.start_new_thread(abc.Threading,(clientSocket, clientAddress))	

serverSocket.close()
