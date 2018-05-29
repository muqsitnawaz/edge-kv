import socket
import pickle
from threading import Thread

class NetworkHandler:
    def __init__(self):
        self.conns = []

        self.edges = {}
        self.users = {}
        self.mappings = {}

    def init_conns(self):
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind((socket.gethostname(), 8080))
        serversocket.listen(5)

        while len(self.conns) != 3:
            sock,addr = serversocket.accept()
            print("got a connection from %s" % str(addr))
            self.conns.append(sock)

    def init_threads(self):
        print('connections',self.conns)
        for conn in self.conns:
            thread = Thread(target = self.process_request, args = [conn])
            thread.start()

    def process_request(self, sock):
        while True:
            req = pickle.loads(sock.recv(1024))
            print('req:', req)

            if req['type'] == 'register':
                eid = str(req['edge_id'])
                loc = req['location']
                http_server = req['http_server']

                # Add edge node to list
                self.edges[eid] = {'socket': sock,'location': loc,'http_server': http_server}
                sock.send(pickle.dumps('Edge node registered'))
            elif req['type'] == 'find':
                uid = req['user_id']
                loc = req['location']

                # Add user to list
                self.users[uid] = {'socket': sock, 'location': loc}

                # Redirect based on client's status old/new
                if uid in self.mappings.keys():
                    u_edge = self.get_edge_by_uid(uid)

                    # Redirect based on client's on loc unchanged/changed
                    if loc == u_edge['location']:
                        sock.send(pickle.dumps(u_edge['http_server']))
                    else:
                        sock.send(pickle.dumps(u_edge['http_server']))
                        dst_edge_id = self.get_edge_id_by_user_location(loc)
                        print('Need to initialize data transfer')
                        u_edge['socket'].send(pickle.dumps({'type': 'transfer', 'user_id': uid, 'edge_id': dst_edge_id}))
                else:
                    for k, v in self.edges.items():
                        if v['location'] == req['location']:
                            self.mappings[uid] = k
                            sock.send(pickle.dumps(v['http_server']))
            elif req['type'] == 'transfer_ack':
                uid = req['user_id']
                eid = req['edge_id']

                # Change mapping and inform client
                self.mappings[uid] = eid
                self.users[uid]['socket'].send(pickle.dumps(self.edges[eid]['http_server']))

            print('edges', self.edges)
            print('users', self.users)
            print('mappings', self.mappings)

    # Takes user_id of a client and returns its node
    def get_edge_by_uid(self, uid):
        return self.edges[self.mappings[uid]]

    def get_edge_id_by_user_location(self, loc):
        for k, v in self.edges.items():
            if v['location'] == loc:
                return k

net = NetworkHandler()
net.init_conns()
net.init_threads()