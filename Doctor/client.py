from socket import socket,AF_INET, SOCK_STREAM, SOCK_DGRAM, timeout, SOL_SOCKET, SO_REUSEADDR
import struct
import sys
from select import select

packer = struct.Struct('20s I I')
input = sys.argv[1]

def goToDoc():
    with socket(AF_INET, SOCK_STREAM) as client:
        doc_server_addr = ('localhost', 10000)
        client.connect(doc_server_addr)

        data = input.encode('utf-8')
        client.sendall(data)
        client.settimeout(1.0)

        answer = client.recv(packer.size)
        unpacked_data = packer.unpack(answer)

        medicine = unpacked_data[0].decode('utf-8')
        amount = unpacked_data[1]
        port = unpacked_data[2]

        client.close()

        if(port != 0):
            goToPharmacy(medicine, port)
        else:
            exit(0)

def goToPharmacy(medicine, port):
    with socket(AF_INET, SOCK_DGRAM) as client:
        pharmacy_server_address = ('localhost', port)
        
        client.sendto(medicine.encode('utf-8'), pharmacy_server_address)
        client.close()
        exit(0)

goToDoc()