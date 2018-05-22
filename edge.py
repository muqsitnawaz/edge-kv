#!/usr/bin/python3

import sys
import socket
import json
import pickle
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

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
        print(fields)
        return

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