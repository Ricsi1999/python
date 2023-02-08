from socket import socket,AF_INET, SOCK_STREAM, SOCK_DGRAM, timeout, SOL_SOCKET, SO_REUSEADDR
import struct
import sys
from select import select

server_addr = ('localhost', 10001)

with socket(AF_INET, SOCK_DGRAM) as server:
	server.setblocking(0)
	server.bind(server_addr)
	server.settimeout(1.0)
	
	while True:
		try:
			values = server.recvfrom(1024)

			print("Kaptam: ", values[0].decode('utf-8'), "tole: ", values[1])
			print("KESZ")

		except timeout:
			pass