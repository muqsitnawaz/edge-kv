from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer

clients = []

def register_client(client_id):
    clients.append()


server = SimpleJSONRPCServer(('localhost', 8080))
server.register_function()
server.register_function(lambda x,y: x+y, 'add')
server.register_function(lambda x: x, 'ping')

server.serve_forever()
