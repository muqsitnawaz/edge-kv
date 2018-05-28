import socket
import pickle                   

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind((socket.gethostname(), 8080))                                  
serversocket.listen(5)

edges = {}
users = {}
mappings = {}

def add_edge(sock, req):
    edges[str(req['edge_id'])] = {'socket': sock, 'location': req['location'], 
            'http_server': req['http_server']}

def add_user(req):
    users[str(req['user_id'])] = {'location': req['location']}

# Takes user_id of a client and returns its node
def get_edge_by_uid(uid):
    return edges[mappings[uid]]

def get_edge_id_by_user_location(loc):
    for k, v in edges.items():
        if v['location'] == loc:
            return k

def process_request(sock, data):
    req = pickle.loads(data)
    print('req: ', req)

    if (req['type'] == 'register'):
        add_edge(sock, req)
        sock.send(pickle.dumps('Edge node registered'))
        print('edges', edges)
    elif (req['type'] == 'find'):
        add_user(req)

        uid = req['user_id']
        loc = req['location']

        # Redirect based on location
        if uid in mappings.keys():
            u_edge = get_edge_by_uid(uid)

            # Redirect exisiting client
            if loc == u_edge['location']:
                sock.send(pickle.dumps(u_edge['http_server']))
            else:
                sock.send(pickle.dumps(u_edge['http_server']))
                dst_edge_id = get_edge_id_by_user_location(loc)
                print('Need to initialize data transfer')
                u_edge['socket'].send(pickle.dumps({'type': 'transfer', 'user_id': uid, 'edge_id': dst_edge_id}))
        else:
            for k, v in edges.items():
                if v['location'] == req['location']:
                    mappings[req['user_id']] = k
                    sock.send(pickle.dumps(v['http_server']))

        print('users', users)
        print('mappings', mappings)
while True:
   sock,addr = serversocket.accept()
   print("Got a connection from %s" % str(addr))
   process_request(sock, sock.recv(1024))