#!/usr/bin/python

import socket
import sys
import threading
import pickle

m = 8

class Node():
	"""
	Chord Node Class: contains functionality for a node in a Chord P2P network
	"""

	def __init__(self, nodeID, host, port):
		self.nodeID = nodeID
		self.host = host
		self.port = port
		self.finger_table = []	# contains finger pointers for this node
		self.predecessor = 0
		self.key_store = {}

		self.__start()			# open listener socket
		print "Node " + str(nodeID) + " started"

	def join(self, otherNodeID):
		if otherNodeID:
			init_finger_table(otherNodeID)
			update_others(otherNodeID)
			# move keys in (predecessor, n] from successor
		else:
			# init all fingers to self
			for i in range(1, 8):
				self.finger_table.append(self.nodeID) 

	def init_finger_table(self, otherNodeID):
		# self.finger_table[0] = self.find_successor(otherNodeID)
		# self.predecessor = successor.predecessor
		# successor.predecessor = self.nodeID
		# for i in range()
		pass

	def find_successor(self, args):
		pass

	def update_others(self, otherNodeID):
		pass

	def update_finger_table2(self, otherNodeID, index):
		pass

	def update_finger_table(self, args):
		pass

	def update_predecessor(self, args):
		pass

	def transfer_keys(self, args):
		pass

	# for testing purposes only
	def send_message(self, message, dest_host, dest_port):
		self.__send_message(message, dest_host, dest_port)

	# for testing purposes only
	def hi(self, args):
		print str(self.nodeID) + " - hi :)"

	# start the listen thread
	def __start(self):
		listenerThread = threading.Thread(target=self.__listen)
		listenerThread.setDaemon(True)
		listenerThread.start()

	def __send_message(self, message, dest_host, dest_port):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.sendto(pickle.dumps(message), (dest_host, dest_port))
		sock.close()

	def __listen(self):
		# Create a socket that will receive all incoming messages to this node
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.bind((self.host, self.port))

		# loop forever, listening to incoming data
		while(1):
			# Receive incoming data
			received = sock.recv(1024)

			# if we've received data, process it
			if received:
				self.__process_message(pickle.loads(received))

	def __process_message(self, message):
		fn = getattr(self, message.function)	# get fn pointer
		fn(message.args)						# call fn


class Message():
	"""
	Class that is used to communicate between nodes in the Chord network
	"""

	def __init__(self, function, args):
		self.function = str(function).lower() if function else None
		self.args = args if args else None
