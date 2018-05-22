import socket
import json
import pickle                   

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind((socket.gethostname(), 8001))                                  
serversocket.listen(5)

clients = []

def process_request(csock, data):
    req = pickle.loads(data)

    if (req['type'] == 'register'):
        clients.append({'socket': csock, 'client_id': req['client_id']})
        csock.send(pickle.dumps('Client registered'))
        print(clients)

while True:
   csock,addr = serversocket.accept()
   print("Got a connection from %s" % str(addr))
   process_request(csock, csock.recv(1024))