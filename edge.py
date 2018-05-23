#!/usr/bin/python3

import sys
import socket
import json
import pickle
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

ss_path = './data/'+sys.argv[1]

def convert(data):
    if isinstance(data, bytes):  return data.decode('ascii')
    if isinstance(data, dict):   return dict(map(convert, data.items()))
    if isinstance(data, tuple):  return map(convert, data)
    if isinstance(data, list):   return convert(data[0])
    return data

def write_to_ss(fields):
    table_path = Path(ss_path+'/'+fields['user_id'])

    if table_path.is_file():
        user_data = {}

        with open(table_path, 'rb') as file:
            user_data = pickle.loads(file.read())
            print(user_data)

        with open(table_path, 'wb') as file:
            user_data[fields['key']] = fields['value']
            file.write(pickle.dumps(user_data))
        
        print('Done')

def read_from_ss(fields):
    table_path = Path(ss_path+'/'+fields['user_id'])

    if table_path.is_file():
        user_data = {}

        with open(table_path, 'rb') as file:
            user_data = pickle.loads(file.read())
            if fields['key'] in user_data.keys():
                return user_data[fields['key']]
            else:
                return 'Bad key'


def connect_to_dc(sock, port):
    sock.connect((socket.gethostname(), port))
    data = pickle.dumps({'type': 'register', 'client_id': sys.argv[1]})
    sock.send(data)
    msg = sock.recv(1024)
    print(pickle.loads(msg))

class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        message = "Hello world!"
        self.wfile.write(bytes(message, "utf8"))
        return

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        fields = parse_qs(self.rfile.read(content_length))
        fields = convert(fields)

        if 'user_id' in fields.keys():
            if 'value' in fields.keys():
                write_to_ss(fields)
                self.send_resp("Success")
            else:
                value = read_from_ss(fields)
                self.send_resp(value)

        else:
            self.send_resp("Bad user_id")
        return

    def send_resp(self, message):
        self.wfile.write(bytes(message, "utf8"))

def run_http():
  server_address = ('127.0.0.1', 9001)
  httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
  print('HTTP server started...')
  httpd.serve_forever()

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connect_to_dc(sock, 8001)
    run_http()

if __name__ == '__main__':
    main()