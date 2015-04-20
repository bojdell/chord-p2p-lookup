#!/usr/bin/python

import socket
import sys
import threading
import pickle
import math
import time
import random

m = 8
BASE_PORT = 5000
DEFAULT_HOST = "localhost"
DEBUG_MODE = False
output_file = None

test_mode = False
current_repetition = 0
current_phase = 0
phase_1_message_total = 0
phase_2_message_total = 0
num_joins = 0
num_finds = 0

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
		self.keys = {}

		self.__start()			# open listener socket
		if DEBUG_MODE:	# TODO: PRINT
			print "Node " + str(nodeID) + " started"

	def __str__(self):
		result = "Node: " + str(self.nodeID) + "\n"
		result += "Finger Table: " + str(self.finger_table) + "\n"
		result += "Predecessor: " + str(self.predecessor) + "\n"
		# result += "Keys: " + str(self.keys) + "\n"
		return result

	def join(self, otherNodeID):
		self.init_finger_table(otherNodeID)
		self.update_others()
		
		# get keys in range [self.predecessor + 1, self.nodeID], inclusive, from successor
		successor = self.finger_table[1]
		msg = Message("remove_keys", [self.predecessor + 1, self.nodeID], self.nodeID, None)
		self.__send_message(msg, DEFAULT_HOST, BASE_PORT+successor)
		self.__listen_for_response()
		self.keys = set(range(self.predecessor + 1, self.nodeID + 1))

	# removes keys in range [start, end], inclusive
	def remove_keys(self, start, end):
		self.keys = self.keys.difference(range(start, end + 1))

	# adds keys in range [start, end], inclusive
	def add_keys(self, start, end):
		self.keys = self.keys.union(range(start, end + 1))

	def find(self, key):
		return self.find_successor(key)

	def leave(self):
		successor = self.finger_table[1]
		for i in range(1,9):
			n = (self.nodeID - pow(2,i-1)) % 256
			p = self.find_predecessor(n)
			# tell p to remove this node
			msg = Message("remove_node", [self.nodeID,i,successor], self.nodeID, None)
			self.__send_message(msg, DEFAULT_HOST, BASE_PORT+p)
			self.__listen_for_response()
		msg = Message("set_predecessor", [self.predecessor], self.nodeID, None)
		self.__send_message(msg, DEFAULT_HOST, BASE_PORT+successor)
		self.__listen_for_response()

		# give keys in range [self.predecessor + 1, self.nodeID], inclusive, to successor
		successor = self.finger_table[1]
		msg = Message("add_keys", [self.predecessor + 1, self.nodeID], self.nodeID, None)
		self.__send_message(msg, DEFAULT_HOST, BASE_PORT+successor)
		self.__listen_for_response()

	def keys_to_string(self):
		result = ""
		for key in self.keys:
			result += str(key) + " "
		return result.strip()

	def remove_node(self, nodeID, index, replace_nodeID):
		if self.finger_table[index] == nodeID:
			self.finger_table[index] = replace_nodeID
			msg = Message("remove_node", [nodeID,index,replace_nodeID], self.nodeID, None)
			self.__send_message(msg, DEFAULT_HOST, BASE_PORT+self.predecessor)
			self.__listen_for_response()

	def init_finger_table(self, otherNodeID):
		start =  self.nodeID + 1
		# find node's successor
		msg = Message("find_successor", [start], self.nodeID, None)
		self.__send_message(msg, DEFAULT_HOST, BASE_PORT+otherNodeID)
		successor = self.__listen_for_response()
		# get predecessor from successor
		msg = Message("get_predecessor", None, self.nodeID, None)
		self.__send_message(msg, DEFAULT_HOST, BASE_PORT+successor)
		predecessor = self.__listen_for_response()
		self.predecessor = predecessor
		# set self as successor's new predecessor
		msg = Message("set_predecessor", [self.nodeID], self.nodeID, None)
		self.__send_message(msg, DEFAULT_HOST, BASE_PORT+successor)
		self.__listen_for_response()
		# start filling in finger table
		self.finger_table[1] = successor
		for i in range(1,8):
			start = (self.nodeID + pow(2,i)) % 256
			if start in range(self.nodeID, self.finger_table[i]):
				self.finger_table[i+1] = self.finger_table[i]
			else:
				msg = Message("find_successor", [start], self.nodeID, None)
				self.__send_message(msg, DEFAULT_HOST, BASE_PORT)
				successor = self.__listen_for_response()
				self.finger_table[i+1] = successor
		for i in range(1,9):
			start = (self.nodeID + pow(2,i-1)) % 256
			if DEBUG_MODE:	# TODO: PRINT
				print "start= " + str(start) + " successor=" + str(self.finger_table[i])

	def update_others(self):
		for i in range (1,9):
			n = (self.nodeID - pow(2,i-1)) % 256
			msg = Message("find_predecessor", [n], self.nodeID, None)
			self.__send_message(msg, DEFAULT_HOST, BASE_PORT)
			p = self.__listen_for_response()
			msg = Message("update_finger_table", [self.nodeID,i], self.nodeID, None)
			self.__send_message(msg, DEFAULT_HOST, BASE_PORT+p)
			self.__listen_for_response()

	def update_finger_table(self, otherNodeID, index):
		if otherNodeID == self.nodeID:
			return
		if otherNodeID in range(self.nodeID, self.finger_table[index]):
			if DEBUG_MODE:	# TODO: PRINT
				print "updating node " + str(self.nodeID) + " at entry i=" + str(index) + " to " + str(otherNodeID)
			self.finger_table[index] = otherNodeID
			p = self.predecessor
			msg = Message("update_finger_table", [otherNodeID, index], self.nodeID, None)
			self.__send_message(msg, DEFAULT_HOST, BASE_PORT+p)
			self.__listen_for_response()
		elif (otherNodeID >= self.nodeID) and (self.finger_table[index] == 0):
			if DEBUG_MODE:	# TODO: PRINT
				print "updating node " + str(self.nodeID) + " at entry i=" + str(index) + " to " + str(otherNodeID)
			self.finger_table[index] = otherNodeID
			p = self.predecessor
			msg = Message("update_finger_table", [otherNodeID, index], self.nodeID, None)
 			self.__send_message(msg, DEFAULT_HOST, BASE_PORT+p)
			self.__listen_for_response()

	def get_successor(self):
		return self.finger_table[1]

	def get_predecessor(self):
		return self.predecessor

	def set_predecessor(self, predecessor):
		self.predecessor = predecessor

	def find_successor(self, nodeID):
		n = self.find_predecessor(nodeID)
		if n == self.nodeID:
			return self.finger_table[1]
		else:
			msg = Message("get_successor", None, self.nodeID, None)
			self.__send_message(msg, DEFAULT_HOST, BASE_PORT+n)
			successor = self.__listen_for_response()
			return successor

	def find_predecessor(self, nodeID):
		n = self.nodeID
		n_successor = self.finger_table[1]

		if nodeID == self.nodeID:
			return self.predecessor
		
		if n == n_successor:
			return n
		while nodeID not in range(n+1, n_successor+1):
			if (nodeID > n) and (n_successor == 0):
				return n
			elif (nodeID == 0) and (n_successor == 0):
				return n
			elif n == self.nodeID:
				n = self.closest_preceding_finger(nodeID)
				if n == self.nodeID:
					n_successor = self.get_successor()
				else:
					msg = Message("get_successor", None, self.nodeID, None)
					self.__send_message(msg, 'localhost', 5000+n)
					n_successor = self.__listen_for_response()
			else:
				msg = Message("closest_preceding_finger", [nodeID], self.nodeID, None)
				self.__send_message(msg,DEFAULT_HOST, BASE_PORT+n)
				n = self.__listen_for_response()
				if n == self.nodeID:
					n_successor = self.finger_table[1]
				else:
					msg = Message("get_successor", None, self.nodeID, None)
					self.__send_message(msg, DEFAULT_HOST, BASE_PORT+n)
					n_successor = self.__listen_for_response()
			if DEBUG_MODE:	# TODO: PRINT
				print "n=" + str(n) + " and ns=" + str(n_successor)
		return n

	def closest_preceding_finger(self, nodeID):
		if DEBUG_MODE:	# TODO: PRINT
			print "called closest_preceding_finger at node " + str(self.nodeID)
		i = 8
		while i > 0:
			if self.finger_table[i] in range(self.nodeID+1, nodeID):
				if DEBUG_MODE:	# TODO: PRINT
					print "the closest_preceding_finger of " + str(nodeID) + " is " + str(self.finger_table[i])
				return self.finger_table[i]
			if self.nodeID > nodeID:
				if (self.finger_table[i] < self.nodeID) and (self.finger_table[i] < nodeID):
					if DEBUG_MODE:	# TODO: PRINT
						print "*the closest_preceding_finger of " + str(nodeID) + " is " + str(self.finger_table[i])
					return self.finger_table[i]
				elif (self.finger_table[i] > self.nodeID) and (self.finger_table[i] > nodeID):
					if DEBUG_MODE:	# TODO: PRINT
						print "**the closest_preceding_finger of " + str(nodeID) + " is " + str(self.finger_table[i])
					return self.finger_table[i]
			i = i-1
		if DEBUG_MODE:	# TODO: PRINT
			print "***the closest_preceding_finger of " + str(nodeID) + " is " + str(self.nodeID)
		return self.nodeID

	# for testing purposes only
	def hi(self, args):
		if DEBUG_MODE:	# TODO: PRINT
			print str(self.nodeID) + " - hi :)"

	# start the listen thread
	def __start(self):
		listenerThread = threading.Thread(target=self.__listen)
		listenerThread.setDaemon(True)
		listenerThread.start()

	# send message to specified host/port
	def __send_message(self, message, dest_host, dest_port):
		global phase_1_message_total
		global phase_2_message_total
		if current_phase == 1:
			phase_1_message_total += 1
		elif current_phase == 2:
			phase_2_message_total += 1
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
				self.__process_message(message)

	# listen for incoming responses before moving on
	def __listen_for_response(self):
		# Create a socket that will receive all incoming messages to this node
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.bind((self.host, 6000+self.nodeID))

		while 1:
			response = sock.recv(1024)

			if response:
				response_msg = pickle.loads(response)
				return response_msg.return_val

	def __process_message(self, message):
		fn = getattr(self, message.function)	# get fn pointer
		if message.args:
			if len(message.args) == 1:
				return_val = fn(message.args[0])
			elif len(message.args) == 2:
				return_val = fn(message.args[0],message.args[1])
			elif len(message.args) == 3:
				return_val = fn(message.args[0],message.args[1],message.args[2])
		else:
			return_val = fn()
		message.return_val = return_val
		if DEBUG_MODE:	# TODO: PRINT
			print "returning " + str(message.return_val)
		self.__send_message(message, DEFAULT_HOST, 6000+message.src_nodeID)


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
		first_node = Node(0, DEFAULT_HOST, BASE_PORT)
		for i in range(1,9):
			first_node.finger_table[i] = 0
		first_node.keys = set(range(0, 256))
		self.nodes[0] = first_node
		# start the coordinator thread
		coordThread = threading.Thread(target=self.__coordinate)
		coordThread.setDaemon(True)
		coordThread.start()

	# get a random ID that is not currently in the network
	def __getRandomNewID(self):
		newNodeID = random.randrange(0, 256)
		while newNodeID in self.nodes:
			newNodeID = random.randrange(0, 256)
		return newNodeID

	# get a random ID that is currently in the network
	def __getRandomExistingID(self):
		return random.choice(self.nodes.keys())

	# generate an automated join command
	def __automateJoin(self):
		global num_joins
		num_joins += 1
		# get a random ID that is not currently in network
		newNodeID = self.__getRandomNewID()
		# generate join command
		return "join " + str(newNodeID)

	# generate an automated find command
	def __automateFind(self):
		global num_finds
		num_finds += 1
		# get random node
		randNode = self.__getRandomExistingID()
		# get random key
		randKey = random.randrange(0, 256)
		# generate find command
		return "find " + str(randNode) + " " + str(randKey)
		
	def __coordinate(self):
		global current_phase
		global current_repetition
		global P
		global F
		global N
		global num_joins
		global num_finds
		global current_phase

		command = ""

		raw_input("Press enter to begin sending messages...")

		while 1:
			if test_mode:

				# if phase 1, perform node joins
				if current_phase == 1:
					# if we've added enough nodes, proceed to phase 2
					if int(num_joins) == int(P):
						print "Joins complete"
						current_phase = 2
						# else, perform a find at a random node for a random value
						command = self.__automateFind()
					else:
						# automate join command
						command = self.__automateJoin()
				elif int(current_phase) == 2:
					# if we've performed enough finds, proceed to next repetition
					if int(num_finds) == int(F):
						current_repetition += 1
						# end after N repetitions, print computed data
						if current_repetition == N:
							print "=== Simulation Completed ==="
							print "phase_1_message_total avg: " + str(phase_1_message_total / N)
							print "phase_2_message_total avg: " + str(phase_2_message_total / N)
							break
						else:
							print "Finds complete, resetting"
							# reset test vars
							initTestVars()
							# automate join command
							command = self.__automateJoin()
					# else, perform a find at a random node for a random value
					else:
						# automate find command
						command = self.__automateFind()
				# print out generated command before executing it below
				print command
			# if we're not simulating, read in a command from terminal
			else:
				command = raw_input()
			command_args = command.split()

			if command_args[0] == "join":
				nodeID = int(command_args[1])
				if nodeID in self.nodes.keys():
					if DEBUG_MODE:	# TODO: PRINT
						print "Node " + str(nodeID) + " already exists!"
				else:
					new_node = Node(nodeID,DEFAULT_HOST,BASE_PORT+nodeID)
					new_node.join(min(self.nodes))
					self.nodes[nodeID] = new_node

			elif command_args[0] == "find":
				nodeID = int(command_args[1])
				key = int(command_args[2])
				print self.nodes[nodeID].find(key)

			elif command_args[0] == "leave":
				nodeID = int(command_args[1])
				self.nodes[nodeID].leave()
				del self.nodes[nodeID]

			elif command_args[0] == "show":
				nodeID = int(command_args[1])
				result = str(nodeID) + " " + self.nodes[nodeID].keys_to_string() + "\n"
				if output_file:
					output_file.write(result)
				else:
					print result,

			elif command_args[0] == "show-all":
				for nodeID in sorted(self.nodes.iterkeys()):
					result = str(nodeID) + " " + self.nodes[nodeID].keys_to_string() + "\n"
					if output_file:
						output_file.write(result)
					else:
						print result,

			elif command_args[0] == "finger":
				nodeID = int(command_args[1])
				node = self.nodes[nodeID]
				for i in range(1,9):
					start = (node.nodeID + pow(2,i-1)) % 256
					if DEBUG_MODE:	# TODO: PRINT
						print "start=" + str(start) + " successor=" + str(node.finger_table[i])

			# for debugging
			elif command_args[0] == "print":
				nodeID = int(command_args[1])
				print self.nodes[nodeID]

			# for debugging
			elif command_args[0] == "print-all":
				for nodeID in self.nodes.keys():
					print self.nodes[nodeID]

			# let us know our command is finished executing
			print "=== Command Executed ==="

def incrGlobal():
	num_joins = num_joins + 1

# initialize variables used in simulation
def initTestVars():
	global current_phase
	global num_joins
	global num_finds

	current_phase = 1
	num_joins = 0
	num_finds = 0
	random.seed(time.time())

if __name__ == "__main__":
	global P
	global F
	global N
	if len(sys.argv) > 1:
		# check if we should output show commands to file
		if sys.argv[1] == "-g":
			output_file = open(sys.argv[2], 'w')
		# check if we should be simulating
		elif sys.argv[1] == "-t":
			test_mode = True
			P = sys.argv[2]
			F = sys.argv[3]
			N = 5
			initTestVars()

	coord = Coordinator()
	# add node zero before starting the thread
	coord.start()
	while 1:
		pass
