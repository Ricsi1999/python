from socket import socket,AF_INET, SOCK_STREAM, timeout, SOL_SOCKET, SO_REUSEADDR
import struct
import sys
from select import select
import random

class Pair(object):
    problem = ""
    medicine = ""

    def __init__(self, problem, medicine):
        self.problem = problem
        self.medicine = medicine

server_addr = ('localhost', 10000)
pharmacy_port = 10001
packer = struct.Struct('20s I I')
list = []

def createList():
	global list
	length = len(sys.argv)
	for i in range(1, length, 2):
		list.append(Pair(sys.argv[i], sys.argv[i + 1]))

def getMedicine(key):
	med = ""
	amount = 0
	port = 0
	found = False

	for l in list:
		if key == l.problem:
			found = True
			med = l.medicine
			amount = random.randint(1, 3)
			port = pharmacy_port
			break

	if not found:
		med = "NINCS"

	return med, amount, port


with socket(AF_INET, SOCK_STREAM) as server:
	server.setblocking(0)
	server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
	server.bind(server_addr)
	server.listen(5)

	createList()

	socketek = [server]
	timeout = 1
	
	while socketek:
		try:
			r,w,e = select(socketek, socketek, socketek, 1)
			
			if not (r or w or e):
				continue
				
			for s in r:
				if s is server:
					client, client_addr = s.accept()
					client.setblocking(0)
					socketek.append(client)

				else:
					try:
						data = s.recv(1024)
						if not data:
							socketek.remove(s)
							if s in w:
								w.remove(s)
							s.close()

						else:
							key = data.decode('utf-8')

							values = getMedicine(key)
							med = values[0].encode('utf-8')
							amount = values[1]
							addr = values[2]

							s.sendall(packer.pack(med, amount, addr))
								
					except:
						print("socket hiba")
						socketek.remove(s)
						if s in w:
							w.remove(s)
						s.close()
						exit(0)
						

		except KeyboardInterrupt:
			for i in socketek:
				i.close()
			socketek = []
			exit(0)