import socket
import sys
import threading
import pickle

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

    def join(self, otherNodeID):
    	pass

    def init_finger_table(self, otherNodeID):
    	pass

    def update_others(self, otherNodeID):
    	pass

    def update_finger_table(self, otherNodeID, index):
    	pass

    # start the listen thread
    def start(self):
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
    	pass


class Message():
	"""
    Class that is used to communicate between nodes in the Chord network
    """

    def __init__(self, command):
    	self.command = str(command).lower() if command else None
    	