import socketserver
import socket
import json
import os
import sys

NODEID = ""
ADDRESS = "127.0.0.1"
PORT = 0 

MASTER_ADDRESS = "127.0.0.1"
MASTER_PORT = 8080

CURRENT_DIRECTORY = os.getcwd()
BUCKET_NAME = "FileServerBucket"         #文件储存路径
BUCKET_PATH = os.path.join(CURRENT_DIRECTORY, BUCKET_NAME)

def dfsOpen(docname):
    path = os.path.join(BUCKET_PATH, docname)
    exists = os.path.isfile(path)
    return exists

def dfsRead(docname):
    path = os.path.join(BUCKET_PATH, docname)
    file_handle = open(path, "r")
    data = file_handle.read()
    return data

def dfsWrite(docname, data):
    path = os.path.join(BUCKET_PATH, docname)
    file_handle = open(path, "w+")
    file_handle.write(data)

class ThreadedHandler(socketserver.BaseRequestHandler):
    '''
    : SocketServer 网络服务框架组件
    : 继承自类socketserver.BaseRequestHandler，该组件协调处理文件路径服务器与锁服务器之间的通信，负责处理文件路径服务器发来的请求报文
    '''
    def handle(self):
        msg = self.request.recv(1024)
        print(msg)

        msg = json.loads(msg)
        requestType = msg['request']
        response = ""

        if requestType == "open":
            exists = dfsOpen(msg['docname'])
            response = json.dumps({"response": requestType, "docname": msg['docname'], "isFile": exists, "address": ADDRESS, "port": PORT})
        elif requestType == "close":
            response = json.dumps({"response": requestType, "address": ADDRESS, "port": PORT})
        elif requestType == "read":
            data = dfsRead(msg['docname'])
            response = json.dumps({"response": requestType, "address": ADDRESS, "port": PORT, "data": data})
        elif requestType == "write":
            dfsWrite(msg['docname'], msg['data'])
            response = json.dumps({"response": requestType, "address": ADDRESS, "port": PORT, "uuid": NODEID})
        else:
            response = json.dumps({"response": "Error", "error": requestType+" is not a valid request", "address": ADDRESS, "port": PORT})

        self.request.sendall(response.encode())


class FileServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

if __name__ == '__main__':
    address = (ADDRESS, PORT)
    server = FileServer(address, ThreadedHandler)
    PORT = server.socket.getsockname()[1]

    msg = json.dumps({"request": "dfsjoin", "uuid": NODEID, "address": ADDRESS, "port": PORT})

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_ADDRESS, MASTER_PORT))
    sock.sendall(msg.encode())
    response = sock.recv(1024)
    #print(type(response))
    sock.close()

    data = json.loads(str(response))
    NODEID = data['uuid']

    print("File Server " + NODEID + " is listening on " + ADDRESS + ":" + str(PORT))

    server.serve_forever()