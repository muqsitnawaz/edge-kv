#!/usr/bin/python3

import sys
import socket
import pickle
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--ip", "-i", type=str, help="ip addr", default="localhost")
parser.add_argument("--port", "-p", type=int, help="port number", default=8001)
parser.add_argument("--edge_id", "-e", type=int, help="node id", default=1)
parser.add_argument("--location", "-l", type=str, help="node location", default="")
args = parser.parse_args()

class StableStorage:
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

SS = StableStorage(args.edge_id)

# Converts a dictionary of bytes to ascii
def convert_fs(data):
    if isinstance(data, bytes):  return data.decode('ascii')
    if isinstance(data, dict):   return dict(map(convert_fs, data.items()))
    if isinstance(data, tuple):  return map(convert_fs, data)
    if isinstance(data, list):   return convert_fs(data[0])
    return data

# Connects to the data center
def connect_to_dc(sock, port, edge_id, location, serv_ip, serv_port):
    sock.connect((socket.gethostname(), port))
    data = pickle.dumps({'type': 'register', 'edge_id': edge_id, 'location': location, 
        'http_server': (serv_ip, serv_port)})
    sock.send(data)
    msg = sock.recv(1024)
    print(pickle.loads(msg))

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

# Runs the http server
def run_http(ip, port):
  server_address = (ip, port)
  httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
  print('HTTP server started...')
  httpd.serve_forever()

# Parses args, and runs services
def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connect_to_dc(sock, 8080, args.edge_id, args.location, args.ip, args.port)
    run_http(args.ip, args.port)

if __name__ == '__main__':
    main()