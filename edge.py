#!/usr/bin/python3

import os
import sys
import json
import socket
import pickle
from threading import Thread
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--edge_id", "-e", type=int, help="node id", default=1)
args = parser.parse_args()

class StorageHandler:
    def __init__(self, edge_id):
        self.path = './data/' + str(edge_id)
        print('Stable storage started')

    def read(self, fields):
        table_path = Path(self.path+'/'+fields['user_id'])

        if table_path.is_file():
            user_data = {}

            with open(table_path, 'rb') as file:
                user_data = pickle.loads(file.read())
                if fields['key'] in user_data.keys():
                    return user_data[fields['key']]
                else:
                    return 'Bad key'
        else:
            return 'Bad key'

    def write(self, fields):
        table_path = Path(self.path+'/'+fields['user_id'])

        if table_path.is_file():
            user_data = {}

            with open(table_path, 'rb') as file:
                user_data = pickle.loads(file.read())

            with open(table_path, 'wb') as file:
                user_data[fields['key']] = fields['value']
                file.write(pickle.dumps(user_data))
        else:
            print('Creating new user')
            user_data = {fields['key']: fields['value']}
            with open(table_path, 'wb') as file:
                file.write(pickle.dumps(user_data))
                file.close()
            print('Done')

    def read_tablet(self, uid):
        print('Reading tablet', uid)
        table_path = Path(self.path+'/'+str(uid))
        user_data = None
        if table_path.is_file():
            with open(table_path, 'rb') as file:
                user_data = pickle.loads(file.read())
        return user_data

    def write_tablet(self, uid, user_data):
        print('Writing tablet', uid)
        table_path = Path(self.path+'/'+str(uid))
        with open(table_path, 'wb') as file:
            file.write(pickle.dumps(user_data))

    def delete_tablet(self, uid):
        print('Deleting tablet',uid)
        table_path = Path(self.path+'/'+str(uid))
        try:
            os.remove(table_path)
        except FileNotFoundError:
            print("tablet not found")

SS = StorageHandler(args.edge_id)

class NetworkHandler:
    def __init__(self, config):
        # Thread.__init__(self)
        self.config = config
        self.conns = {}

    def init_conns(self):
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serversocket.bind((socket.gethostname(), self.config[str(args.edge_id)]['pport']))                                  
        self.serversocket.listen(5)

        while len(self.conns) != 2 - args.edge_id:
            sock,addr = self.serversocket.accept()
            print("got a connection from %s" % str(addr))
            pid = pickle.loads(sock.recv(1024))
            print('edge id', pid)
            self.conns[str(pid)] = sock

        for k, v in self.config.items():
            if (int(k) < args.edge_id):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((socket.gethostname(), v['pport']))
                sock.send(pickle.dumps(args.edge_id))
                print('Made connection to ', v['ip'], v['pport'])
                self.conns[str(k)] = sock

    def init_threads(self):
        print('connections',self.conns)
        for k in self.conns.keys():
            thread = Thread(target = self.process, args = [k])
            thread.start()

    def init_dc_connc(self, loc, ip, port):
        self.dcsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dcsock.connect((socket.gethostname(), 8080))
        
        # Register edge with dc
        self.dcsock.send(pickle.dumps({'type':'register','edge_id':args.edge_id,'location':loc,'http_server':(ip, port)}))
        msg = self.dcsock.recv(1024)
        print(pickle.loads(msg))

        # Starting conn thread
        thread = Thread(target = self.process_dc)
        thread.start()

    def process(self, rid):
        print('processing thread', self.conns[rid])
        sock = self.conns[rid]

        while True:
            req = pickle.loads(sock.recv(1024))
            print(rid, ':', req)

            if req['type'] == 'user_data':
                uid = req['user_id']
                user_data = req['user_data']

                SS.write_tablet(uid, user_data)
                sock.send(pickle.dumps({'type':'user_data_ack','user_id':uid}))
            elif req['type'] == 'user_data_ack':
                uid = req['user_id']

                SS.delete_tablet(uid)
                self.dcsock.send(pickle.dumps({'type':'transfer_ack','user_id':uid,'edge_id':rid}))

    def process_dc(self):
        while True:
            req = pickle.loads(self.dcsock.recv(1024))
            print('dc:',req)

            if req['type'] == 'transfer':
                uid = req['user_id']
                rid = req['edge_id']

                print('Sending user', uid, 'data to edge', rid)
                user_data = SS.read_tablet(uid)
                self.conns[rid].sendall(pickle.dumps({'type':'user_data','user_id':uid,'user_data':user_data}))

# Convert a dictionary of bytes to ascii
def convert_fs(data):
    if isinstance(data, bytes):  return data.decode('ascii')
    if isinstance(data, dict):   return dict(map(convert_fs, data.items()))
    if isinstance(data, tuple):  return map(convert_fs, data)
    if isinstance(data, list):   return convert_fs(data[0])
    return data

class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def send_resp(self, message):
        self._set_headers()
        self.wfile.write(bytes(message, "utf8"))

    def do_GET(self):
        self.send_resp("Hello world!")
        return

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        fields = convert_fs(parse_qs(self.rfile.read(content_length)))

        if 'user_id' in fields.keys():
            if 'value' in fields.keys():
                SS.write(fields)
                self.send_resp("Success")
            else:
                value = SS.read(fields)
                self.send_resp(value)
        else:
            self.send_resp("Bad user_id")
        return

# Run the http server
def run_http(ip, port):
    server_address = (ip, port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('HTTP server starting on',server_address)
    httpd.serve_forever()

# Parse args and run services
def main():
    with open('./config.json') as config_file:    
        config = json.load(config_file)
    m_config = config[str(args.edge_id)]

    net = NetworkHandler(config)
    net.init_conns()
    net.init_threads()
    net.init_dc_connc(m_config['location'], m_config['ip'], m_config['port'])

    run_http(m_config['ip'], m_config['port'])

if __name__ == '__main__':
    main()