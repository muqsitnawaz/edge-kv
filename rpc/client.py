import jsonrpclib
server = jsonrpclib.Server('http://localhost:8080')
print(server.add(5,6))
