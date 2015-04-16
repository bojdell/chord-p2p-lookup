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
				if nodeID in nodes.keys():
					print "Node " + nodeID + " already exists!"
					break
				nodeID = int(command_args[1])
				self.join(nodeID)

			elif command_args[0] == "find":
				nodeID = int(command_args[1])
				key = int(command_args[2])
				nodes[nodeID].find(key)

			elif command_args[0] == "leave":
				nodeID = int(command_args[1])
				nodes[nodeID].find(key)

			elif command_args[0] == "show":
				nodeID = int(command_args[1])
				node[nodeID].print_keys()

			elif command_args[0] == "show-all":
				for nodeID in self.nodes.keys():
					node[nodeID].print_keys()

if __name__ == "__main__":
	coord = Coordinator()
	# add node zero before starting the thread
	coord.start()
