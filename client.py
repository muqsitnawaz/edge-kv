import sys
import socket
import pickle
import requests
import argparse
from threading import Thread

parser = argparse.ArgumentParser()
parser.add_argument("--user_id", "-i", type=str, help="ip addr", default="a")
parser.add_argument("--location", "-l", type=str, help="node location", default="")
args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((socket.gethostname(), 8080))
data = pickle.dumps({'type': 'find', 'user_id': args.user_id, 'location': args.location})
sock.send(data)
msg = sock.recv(1024)
edge = pickle.loads(msg)
addr = 'http://'+edge[0]+':'+str(edge[1])

def SockAcpt(sock):
    global addr

    while True:
        edge = pickle.loads(sock.recv(1024))
        addr = 'http://'+edge[0]+':'+str(edge[1])
        print(addr)

thread = Thread(target = SockAcpt, args = [sock])
thread.start()

inp = ""
while inp != '0':
    print('0: quit')
    print('1: read')
    print('2: write')
    print('3: change loc')
    inp = input('')

    if inp == '1':
        k = input('key?')
        r = requests.post(addr, data={'user_id': args.user_id, 'key': k})
        print(r.text)
    elif inp == '2':
        k = input('key?')
        v = input('value?')
        r = requests.post(addr, data={'user_id': args.user_id, 'key': k, 'value': v})
        print(r.text)
    elif inp == '3':
        loc = input('loc?')
        data = pickle.dumps({'type': 'find', 'user_id': args.user_id, 'location': loc})
        sock.send(data)