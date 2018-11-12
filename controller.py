#!/usr/bin/env python
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

import bank_pb2
import socket
import time
import random


branches = {}
branch_name = []

def initBranch(fname,total_balance):
  
	count=0
	BR = bank_pb2.BranchMessage()

	#store branch name, ip , port
	with open(fname) as f:
		for line in f:  
			ip = line.split()[1]    
			#port = line.split()[2]  
			inport= int(line.split()[2].strip('\0'))      
			
			init_branch = BR.init_branch

			branch = init_branch.all_branches.add()

			branch.name=line.split()[0]
			branch.ip=ip
			branch.port=inport
			count=count+1


			temp_list = []
			temp_list.append(ip)
			temp_list.append(inport)
			branches[line.split()[0]] = temp_list
			
			branch_name.append(line.split()[0])
  
	f.close()

  
    	#initial balance of branches
	init_branch.balance = int(int(total_balance) / count)
    
	# Sending init branch message
	print "Sending init_branch message to all branches"
	with open(fname) as f:
		for line in f:
	
			ip = line.split()[1]    
			port = line.split()[2]  
			inport=int(port.strip('\0'))

			try:

				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
				s.connect((ip,int(inport)))
				s.send(BR.SerializeToString())
				print s.recv(100000)

				s.close()
			except:
				print "Not able to connect to " + line + " " +str(sys.exc_info()[0])
				sys.exit(0)

	f.close()



def initSnapshot():
	
	snapshot_id = 1
		
	while 1:
		branch = random.choice(branch_name)
		ip = branches[branch][0]
		port = branches[branch][1]
	
		# Create init snapshot message
		branch_message = bank_pb2.BranchMessage()
		init_snapshot = branch_message.init_snapshot
		init_snapshot.snapshot_id = snapshot_id
	
		#send init snapshot 
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		s.connect((ip,int(port)))	
		s.send(branch_message.SerializeToString())
		print s.recv(10000)
		s.close()
	
		RetrieveSnapshot(snapshot_id)
		snapshot_id += 1


def RetrieveSnapshot(id):
	time.sleep(3)
	#Create Retrieve Message
	branch_message = bank_pb2.BranchMessage()
	retrieve_snapshot = branch_message.retrieve_snapshot
	retrieve_snapshot.snapshot_id = id

	print "snapshot_id: ", id
	for b_name in branch_name:
		
		ip = branches[b_name][0]
		port = branches[b_name][1]
		
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		s.connect((ip,int(port)))
		s.send(branch_message.SerializeToString())
		snapshot = s.recv(100000)
		recv_message = bank_pb2.BranchMessage()
		recv_message.ParseFromString(snapshot)
		
		return_message = recv_message.return_snapshot.local_snapshot
		
		output = b_name + ": " + str(return_message.balance) + ", "
		
		output_list = []
		for name in branch_name:
			if name != b_name:
				output_list.append(name+'->'+b_name)
		for i in range(0,len(output_list)):
			output = output + output_list[i] + ': ' + str(return_message.channel_state[i]) + ', '
		
		output = output[:-2]
		print output
		s.close()
		


if __name__ == '__main__':

	if len(sys.argv) != 3:
	        print "Invalid Parameters: <Total Balanace> <branches.txt>"
	        sys.exit(0)
	else:
	        total_balance = sys.argv[1]
	        fname = sys.argv[2]

	initBranch(fname,total_balance)
	time.sleep(5)
	initSnapshot()
