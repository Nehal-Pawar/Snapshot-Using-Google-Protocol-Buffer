import socket
import os
import sys
import thread
import random
import time
import copy
import threading
from threading import Lock

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

import bank_pb2


class BRANCH:
	branches = {}   # branch name : ip , port
	rec_sockets = {}
	balance = 0
	sender_sockets = {} # receiver branch name : ip port socket_object
	snap = {}
	temp_dict={}
	
	def __init__(self):
		self.balance = 0
		self.balance_Lock = Lock()
		self.receive_amt_Lock = Lock()
		self.send_amt_Lock = Lock()
		self.initial_balance = 0
		self.conn_count = 0
		self.count_lock = Lock()
                self.snap_lock=Lock()
                self.temp_dict_lock=Lock()
		self.record = {}
		self.stop_transfer = {}
		


		
		
	def Amount_Receive(self,clientSocket, clientAddress):
		
		while 1:
			
			
			msg = clientSocket.recv(1024)
			self.receive_amt_Lock.acquire()
			trans_msg = bank_pb2.BranchMessage()

			trans_msg.ParseFromString(msg)
			if trans_msg.HasField('transfer'):
				
				self.balance_Lock.acquire()
				self.balance += trans_msg.transfer.money
				self.balance_Lock.release()

				# Recording incoming channel states
				if self.record[trans_msg.transfer.src_branch] == True:
					recv_channel=trans_msg.transfer.src_branch + '_to_' + trans_msg.transfer.dst_branch
					if recv_channel in self.temp_dict.keys():
					    self.temp_dict_lock.acquire()	
                                            self.temp_dict[recv_channel].append(trans_msg.transfer.money)
                                            self.temp_dict_lock.release()
					else:
                                                self.temp_dict_lock.acquire()
						self.temp_dict[recv_channel] = []
						self.temp_dict[recv_channel].append(trans_msg.transfer.money)
                                                self.temp_dict_lock.release()	
				print "Transfer from " + trans_msg.transfer.src_branch + " to " + trans_msg.transfer.dst_branch + " Balance = " + str(self.balance)
			self.receive_amt_Lock.release()


	def init_transfer(self):
		
		
		time.sleep(2)
		
		i=1
		while 1:
			if self.stop_transfer[sys.argv[1]] == False:
				sleep_time = int(sys.argv[3]) / 1000.0
				random_sleep_time = random.uniform(0, sleep_time)
				time.sleep(random_sleep_time)
			
				self.send_amt_Lock.acquire()
				dest_branch = random.choice(self.sender_sockets.keys())
				withdraw_money = random.randint(0.01 * self.initial_balance, 0.05 * self.initial_balance)

				if self.balance >= withdraw_money:
					self.balance_Lock.acquire()
					self.balance = self.balance - withdraw_money
					self.balance_Lock.release()

					# Generating a transfer message
					branch_message = bank_pb2.BranchMessage()
					transfer_message = branch_message.transfer
					transfer_message.src_branch = sys.argv[1]
					transfer_message.dst_branch = dest_branch
					transfer_message.money = withdraw_money

					# Send transfer message
	
					socket_object = self.sender_sockets[dest_branch][2]
					#print socket_object.stillconnected()
					socket_object.send(branch_message.SerializeToString())
					print 'i = ' + str(i) +' Trasfer to ' + dest_branch + ' Current balance ' + str(self.balance)
					i += 1
				self.send_amt_Lock.release()


	def Threading_Receive(self, clientSocket, clientAddress):
	  	# Receive the message
	  	msg = clientSocket.recv(1024)

		if "Connection" in msg:
			
			# Storing socket information
			self.count_lock.acquire()
			self.conn_count += 1
			self.count_lock.release()

			conn_branch = msg.split()[3]
			temp_list = []
			temp_list.append(clientAddress[0])
			temp_list.append(clientAddress[1])
			temp_list.append(clientSocket)

			self.rec_sockets[conn_branch] = temp_list
			
			#thread.start_new_thread(self.Amount_Receive,(clientSocket, clientAddress))


			amt_rec_thread = threading.Thread(target = self.Amount_Receive, args = (clientSocket, clientAddress))
			amt_rec_tranfer.daemon = True
			amt_rec_tranfer.start()

			
		else:
		  	bankdetails = bank_pb2.BranchMessage()

			bankdetails.ParseFromString(msg)
			

		  	if bankdetails.HasField('init_branch'):

		  		response = 'Init Message received to ' + sys.argv[1]

				# Send the response to client
			  	clientSocket.send(response)

				# Extract balance
				self.balance_Lock.acquire()
				self.balance =  bankdetails.init_branch.balance
				self.initial_balance =  bankdetails.init_branch.balance
				self.balance_Lock.release()
		  		print 'Initial balance = ' + str(self.balance)

				# Extract details of other branches in branches{}
				for each_branch in bankdetails.init_branch.all_branches:
		  			if each_branch.name != sys.argv[1]:
						ip_port = []
						ip_port.append(each_branch.ip)
						ip_port.append(each_branch.port)
		  				self.branches[each_branch.name] = ip_port


						#Create key in  record and mark it as false for each branch
						self.record[each_branch.name] = False
				#print self.branches
				
				#Create key in stop_transfer
				self.stop_transfer[sys.argv[1]] = False

				# connect to all other branches
				for branch_name in self.branches:
					sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sender_socket.connect((self.branches[branch_name][0],int(self.branches[branch_name][1])))
					sender_ip, sender_port = sender_socket.getsockname()
					sender_socket.send('Connection message from ' + sys.argv[1])
					
					list1 = []
					list1.append(sender_ip)
					list1.append(sender_port)
					list1.append(sender_socket)
					self.sender_sockets[branch_name] = list1
						
				
				print 'starting transfer thread'
				#thread.start_new_thread(self.init_transfer,())

				init_tranfer_thread = threading.Thread(target = self.init_transfer)
				init_tranfer.daemon = True
				init_tranfer.start()



				    		
			elif bankdetails.HasField('init_snapshot'):
				#print bankdetails
				response = 'Init Snapshot Message received to ' + sys.argv[1]
				# Send the response to client
			  	clientSocket.send(response)
			  	print 'Init Snapshot Message received'



				#Stop sending transfer messages
				self.stop_transfer[sys.argv[1]] = True
				print 'stopping transfer messages'

			  	# store local balance 
				self.balance_Lock.acquire()
				self.snap[bankdetails.init_snapshot.snapshot_id] = [self.balance]
				self.balance_Lock.release()
				
				print self.snap
				#send marker to all      //try to make method
				for branch_name in self.branches:
					sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sender_socket.connect((self.branches[branch_name][0],int(self.branches[branch_name][1])))
					# Generating a marker message
					branch_message = bank_pb2.BranchMessage()
					marker_message = branch_message.marker
					marker_message.src_branch = sys.argv[1]
					marker_message.dst_branch = branch_name
					marker_message.snapshot_id = bankdetails.init_snapshot.snapshot_id
					sender_socket.send(branch_message.SerializeToString())
		
					#Start recording
					self.record[branch_name] = True


				print 'starting transfer messages'
				self.stop_transfer[sys.argv[1]] = False
				
			elif bankdetails.HasField('marker'):
				


				print bankdetails			
				snap_id = bankdetails.marker.snapshot_id
				recv_channel = bankdetails.marker.src_branch + '_to_' + bankdetails.marker.dst_branch
				
				# First Marker
				if not snap_id in self.snap.keys():

					#Stop sending transfer messages
					self.stop_transfer[sys.argv[1]] = True
					print 'stopping transfer messages'

					print 'First marker message with snap id ' + str(snap_id)
					#time.sleep(1)

					# Record balance and incoming state as empty
				        self.snap_lock.acquire()	
					self.snap[snap_id] = [self.balance]
					self.temp_dict[recv_channel] = []
					if self.temp_dict[recv_channel]:
						sum_temp = sum(self.temp_dict[recv_channel])
					else:
						sum_temp = 0
					self.snap[snap_id].append([recv_channel , sum_temp])
					self.snap_lock.release()

					print 'snap ', self.snap
                                        
					for branch_name in self.branches:
						sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						sender_socket.connect((self.branches[branch_name][0],int(self.branches[branch_name][1])))
						# Generating a marker message
						branch_message = bank_pb2.BranchMessage()
						marker_message = branch_message.marker
						marker_message.src_branch = sys.argv[1]
						marker_message.dst_branch = branch_name
						marker_message.snapshot_id = snap_id
						sender_socket.send(branch_message.SerializeToString())
	
						#Start recording
						self.record[branch_name] = True

					#Start sending transfer messages
					self.stop_transfer[sys.argv[1]] = False
					print 'starting transfer messages'

				else:
					#add temp_dict
					#self.record=False
					#print self.temp_dict
					self.snap_lock.acquire()
					if recv_channel not in self.temp_dict:
						self.temp_dict[recv_channel] = []
                                        print "temp_dict ", self.temp_dict[recv_channel]


					if self.temp_dict[recv_channel]:
						sum_temp = sum(self.temp_dict[recv_channel])
					else:
						sum_temp = 0
					self.snap[snap_id].append([recv_channel , sum_temp])
					
                                        	
                                        self.snap_lock.release()
					print "snap ", self.snap


			elif bankdetails.HasField('retrieve_snapshot'):
				
				snap_id = bankdetails.retrieve_snapshot.snapshot_id
				
						

				branch_message = bank_pb2.BranchMessage()
				return_message = branch_message.return_snapshot
				add_snap = return_message.local_snapshot
				add_snap.snapshot_id = snap_id
				add_snap.balance = self.snap[snap_id][0]


				#add_snap.channel_state.append(1)
				for i in range(1,len(self.snap[snap_id])):
					add_snap.channel_state.append(self.snap[snap_id][i][1])
					#print key
					#if key in snap[snap_id]:
						

				clientSocket.send(branch_message.SerializeToString())
				
				self.snap = {}
				self.temp_dict={}
				for key in self.record:	
					self.record[key] = False
				for key in self.stop_transfer:	 
					self.stop_transfer[key] = False 

	  	# Close the client socket
	  	#clientSocket.close()








if __name__ == '__main__':
	# No command line arguments needed
	if len(sys.argv) != 4:
		print("Usage:", sys.argv[0], "Branch name", "Port number", "Maximum Interval in ms")
		sys.exit()

	branch_object = BRANCH()

	# Create a TCP Server Socket
	serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# Binding socket on an unused port
	serverSocket.bind(('',int(sys.argv[2])))

	# Print host name and port number of server
	host_name = socket.gethostbyname(socket.gethostname())
	server_port = serverSocket.getsockname()[1]
	print host_name + ' listening on port ' + str(server_port)

	serverSocket.listen(10)

	
	while 1:
		# Accepting the client request
		clientSocket, clientAddress = serverSocket.accept()
		#print 'printing socket and addr of incoming messages ' + " "+ str(clientSocket) + " "+ str(clientAddress)
		

		# starting daemon threads
                recv_thread = threading.Thread(target = branch_object.Threading_Receive, args=(clientSocket, clientAddress))
                recv_thread.daemon = True
                recv_thread.start()
		#thread.start_new_thread(branch_object.Threading_Receive,(clientSocket, clientAddress))

	serverSocket.close()
