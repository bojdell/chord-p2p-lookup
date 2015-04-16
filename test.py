#!/usr/bin/python

from Node import Node
from Node import Message

# usage: ??
if __name__ == "__main__":
	node1 = Node(0, "localhost", 5000)
	node2 = Node(5, "localhost", 5001)

	raw_input("Press Enter to contiunue...\n")

	fn = "hi"
	args = []
	msg = Message(fn, args)
	node1.send_message(msg, "localhost", 5001)

	raw_input("Press Enter to contiunue...\n")