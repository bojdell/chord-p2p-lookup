import socket
import sys
import threading

class Coordinator():

	def __init__(self):
		nodes = {}
		port = 5555

	def start(self):
		coordThread = threading.Thread(target=self._coordinate)
		coordThread.setDaemon(True)
		coordThread.start()
		
	def _coordinate(self):
		while 1:
			command = raw_input()
			command_args = command.split()

			if command_args[0] == "join":
				nodeID = int(command_args[1])
				self.join(nodeID)

			elif command_args[0] == "find":
				nodeID = int(command_args[1])
				key = int(command_args[2])
				self.find(nodeID, key)

			elif command_args[0] == "leave":
				nodeID = int(command_args[1])
				self.leave(nodeID)

			elif command_args[0] == "show":
				nodeID = int(command_args[1])
				node[nodeID].print_keys()

			elif command_args[0] == "show-all":
				for nodeID in self.nodes.keys():
					node[nodeID].print_keys()

	def join(self, nodeID):
		if nodeID in nodes.keys():
			print "Node " + nodeID + " already exists!"
			return

		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.sendto("join " + str(nodeID), ('localhost', nodes[0].port))
		
		sock.bind(('localhost', self.port))
		received = sock.recv(1024)
		
		if received == "NACK":
			print "join: Something went wrong."

	def find(self, nodeID, key):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.sendto("find " + str(key), ('localhost', nodes[nodeID].port))
		
		sock.bind(('localhost', self.port))
		received = sock.recv(1024)
		
		if received == "NACK":
			print "find: Something went wrong."

	def leave(self, nodeID):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.sendto("leave", ('localhost', nodes[nodeID].port))
		
		sock.bind(('localhost', self.port))
		received = sock.recv(1024)
		
		if received == "NACK":
			print "leave: Something went wrong."


if __name__ == "__main__":
	coord = Coordinator()
	# add node zero before starting the thread
	coord.start()
