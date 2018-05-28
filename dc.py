import socket
import pickle                   

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind((socket.gethostname(), 8080))                                  
serversocket.listen(5)

edges = {}
clients = {}

def add_edge(req):
    edges[str(req['edge_id'])] = {'socket': csock, 'location': req['location'], 
            'http_server': req['http_server']}

def add_user(req):
    clients[str(req['user_id'])] = {'location': req['location']}

def process_request(csock, data):
    req = pickle.loads(data)
    print('req: ', req)

    if (req['type'] == 'register'):
        add_edge(req)
        csock.send(pickle.dumps('Edge node registered'))
        print(edges)
    elif (req['type'] == 'find'):
        add_user(req)
        for edge in edges.values():
            if edge['location'] == req['location']:
                csock.send(pickle.dumps(edge['http_server']))

while True:
   csock,addr = serversocket.accept()
   print("Got a connection from %s" % str(addr))
   process_request(csock, csock.recv(1024))