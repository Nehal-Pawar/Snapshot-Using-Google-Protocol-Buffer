import socket
import os
import sys

# No command line arguments needed
if len(sys.argv) != 4:
	print("Usage:", sys.argv[0], "Branch name", "Port number", "Maximum Interval in ms")
	sys.exit()

# Create a TCP Server Socket
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Binding socket on an unused port
serverSocket.bind(('',int(sys.argv[2])))

# Print host name and port number of server
host_name = socket.gethostname()
server_port = serverSocket.getsockname()[1]
print host_name + ' listening on port ' + str(server_port)


serverSocket.listen(1)

while 1:
	# Accepting the client request
	clientSocket, clientAddress = serverSocket.accept()

	# Receive the message
	msg = clientSocket.recv(1024)
	#bankdetails.ParseFromString(msg)

	print msg

	response = 'Message recieved to ' + sys.argv[1]
	# Send the response to client
	clientSocket.send(response)

	# Close the client socket
	clientSocket.close()
	

serverSocket.close()
