#!/usr/bin/python

import socket
import sys
import threading
import pickle
import math
import time

class Node():
	"""
	Chord Node Class: contains functionality for a node in a Chord P2P network
	"""

	def __init__(self, nodeID, host, port):
		self.nodeID = nodeID
		self.host = host
		self.port = port
		self.finger_table = {}	# contains finger pointers for this node
		self.predecessor = 0
		self.keys = ()

		self.__start()			# open listener socket
		print "Node " + str(nodeID) + " started"

	def join(self):
		self.init_finger_table(0)
		self.update_others()

	def find(self, key):
		return self.find_successor(key)

	def leave(self):
		successor = self.finger_table[1]
		msg = Message("set_predecessor", [self.predecessor], self.nodeID, None)
		self.__send_message(msg, 'localhost', 5000+successor)
		self.__listen_for_response()
		for i in range(1,9):
			p = self.find_predecessor(self.nodeID - pow(2,i-1))
			# tell p to remove this node
			msg = Message("remove_node", [self.nodeID,i,successor])
			self.__send_message(msg, 'localhost', 5000+p)

	def print_keys(self):
		print "called print keys"
		min_key = self.keys[0]
		max_key = self.keys[1]
		for i in range(min_key, max_key+1):
			print i

	def remove_node(self, nodeID, index, replace_nodeID):
		if self.finger_table[index] == nodeID:
			finger[index] = replace_nodeID
			msg = Message("remove_node", [nodeID,i,replace_nodeID])
			self.__send_message(msg, 'localhost', 5000+self.predecessor)

	def init_finger_table(self, otherNodeID):
		start =  self.nodeID + 1
		# find node's successor
		msg = Message("find_successor", [start], self.nodeID, None)
		self.__send_message(msg, 'localhost', 5000)
		successor = self.__listen_for_response()
		# get predecessor from successor
		msg = Message("get_predecessor", None, self.nodeID, None)
		self.__send_message(msg, 'localhost', 5000+successor)
		predecessor = self.__listen_for_response()
		self.predecessor = predecessor
		# set self as successor's new predecessor
		msg = Message("set_predecessor", [self.nodeID], self.nodeID, None)
		self.__send_message(msg, 'localhost', 5000+successor)
		self.__listen_for_response()
		# start filling in finger table
		self.finger_table[1] = successor
		for i in range(1,8):
			start = pow(2,i) % 256
			if start in range(self.nodeID, self.finger_table[i]):
				self.finger_table[i+1] = self.finger_table[i]
			else:
				msg = Message("find_successor", [start], self.nodeID, None)
				self.__send_message(msg, 'localhost', 5000)
				successor = self.__listen_for_response()
				self.finger_table[i+1] = successor

	def update_others(self):
		for i in range (1,9):
			n = self.nodeID - pow(2,i-1) % 256
			print "find_predecessor of " + str(n)
			p = self.find_predecessor(n)
			msg = Message("update_finger_table", [self.nodeID,i], self.nodeID, None)
			self.__send_message(msg, 'localhost', 5000+p)
			self.__listen_for_response()

	def update_finger_table(self, otherNodeID, index):
		if otherNodeID == self.nodeID:
			return
		if otherNodeID in range(self.nodeID, self.finger_table[index]):
			self.finger_table[index] = otherNodeID
			p = self.predecessor
			msg = Message("update_finger_table", [otherNodeID, index], self.nodeID, None)
			self.__send_message(msg, 'localhost', 5000+p)
			self.__listen_for_response()

	def update_finger_table_MSG(self, args):
		pass

	def update_predecessor_MSG(self, args):
		pass

	def transfer_keys_MSG(self, args):
		pass

	def get_successor(self):
		return self.finger_table[1]

	def get_predecessor(self):
		return self.predecessor

	def set_predecessor(self, predecessor):
		self.predecessor = predecessor

	def find_successor(self, nodeID):
		n = self.find_predecessor(nodeID)
		print "predecessor is " + str(n)
		msg = Message("get_successor", None, self.nodeID, None)
		if n == self.nodeID:
			return self.finger_table[1]
		else:
			self.__send_message(msg, 'localhost', 5000+n)
			successor = self.__listen_for_response()
			print "successor is " + str(successor)
			return successor

	def find_predecessor(self, nodeID):
		n = self.nodeID
		n_successor = self.finger_table[1]
		if n == n_successor:
			return n
		while nodeID not in range(n, n_successor+1): #TODO: how do we find n's successor???
			if nodeID >= n and n_successor == 0:
				return n
			msg = Message("closest_preceding_finger", [nodeID], self.nodeID, None)
			self.__send_message(msg,'localhost', 5000+n)
			n = self.__listen_for_response()
			msg = Message("get_successor", None, self.nodeID, None)
			self.__send_message(msg, 'localhost', 5000+n)
			n_successor = self.__listen_for_response()
		return n

	def closest_preceding_finger(self, nodeID):
		i = 8
		while i > 0:
			print "if " + str(self.finger_table[i]) + " in [" + str(self.nodeID+1) + "," + str(nodeID[0]-1) + "]"
			if self.finger_table[i] in range(self.nodeID+1, nodeID[0]):
				return self.finger_table[i]
			i = i-1
		return self.nodeID

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

	# send message to specified host/port
	def __send_message(self, message, dest_host, dest_port):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.sendto(pickle.dumps(message), (dest_host, dest_port))
		sock.close()

	# listen for incoming messages
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
				time.sleep(0.1)
				message = pickle.loads(received)
				print "Node " + str(self.nodeID) + " received a message: " + message.function
				self.__process_message(message)

	# listen for incoming responses before moving on
	def __listen_for_response(self):
		# Create a socket that will receive all incoming messages to this node
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.bind((self.host, self.nodeID))

		while 1:
			response = sock.recv(1024)

			if response:
				response_msg = pickle.loads(response)
				return response_msg.return_val

	def __process_message(self, message):
		fn = getattr(self, message.function)	# get fn pointer
		if message.args:
			return_val = fn(message.args)		# call fn
		else:
			return_val = fn()
		message.return_val = return_val
		print "returning " + str(message.return_val)
		self.__send_message(message, 'localhost', message.src_nodeID)


class Message():
	"""
	Class that is used to communicate between nodes in the Chord network
	"""

	def __init__(self, function, args, src_nodeID, return_val):
		self.function = str(function).lower() if function else None
		self.args = args if args else None
		self.src_nodeID = src_nodeID
		self.return_val = return_val if return_val else None

class Coordinator():

	def __init__(self):
		self.nodes = {}

	def start(self):
		# initialize node 0
		first_node = Node(0, 'localhost', 5000)
		for i in range(1,9):
			first_node.finger_table[i] = 0
		first_node.keys = (0,255)
		self.nodes[0] = first_node
		# start the coordinator thread
		coordThread = threading.Thread(target=self.__coordinate)
		coordThread.setDaemon(True)
		coordThread.start()
		
	def __coordinate(self):
		raw_input("Press enter to begin sending messages...")

		while 1:
			command = raw_input()
			command_args = command.split()

			if command_args[0] == "join":
				nodeID = int(command_args[1])
				if nodeID in self.nodes.keys():
					print "Node " + nodeID + " already exists!"
					break
				new_node = Node(nodeID,'localhost',5000+nodeID)
				new_node.join()
				nodes[nodeID] = new_node

			elif command_args[0] == "find":
				nodeID = int(command_args[1])
				key = int(command_args[2])
				print self.nodes[nodeID].find(key)

			elif command_args[0] == "leave":
				nodeID = int(command_args[1])
				self.nodes[nodeID].leave()

			elif command_args[0] == "show":
				nodeID = int(command_args[1])
				self.nodes[nodeID].print_keys()

			elif command_args[0] == "show-all":
				for nodeID in self.nodes.keys():
					print nodeID
					self.nodes[nodeID].print_keys()

if __name__ == "__main__":
	coord = Coordinator()
	# add node zero before starting the thread
	coord.start()
	while 1:
		pass

