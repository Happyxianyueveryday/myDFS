import socketserver
import json
import uuid
import random
import time
import os
import sys

ADDRESS = "127.0.0.1"
PORT = 8080

#文件路径服务器使用典型的SocketServer网络服务框架进行组织

FILE_SERVER = {}     #文件服务器列表
FILE_ADDRESS = {}    #文件路径映射列表：将单个文件映射到对应的文件服务器

def fileExistsTest(docname):
    '''
    : 判断指定文件名的文件是否在文件路径映射中
    : docname: str,文件名
    : return -> bool
    '''
    if docname in FILE_ADDRESS:      #判断文件名是否在文件路径映射的键中，若在则指定文件存在，否则指定文件不存在
        return True
    else:
        return False

def getFileAddress(docname):
    '''
    : 获得给定的文件所在的文件服务器地址
    : docname: str,文件名
    : return -> 指定文件所在的文件服务器地址
    '''
    if fileExistsTest(docname):
        return FILE_ADDRESS[docname]   #从文件路径映射列表中读取文件所在的服务器
    else:
        return None

def addFileAddress(docname, nodeID, address, port, timestamp):
    '''
    : 向指定的文件服务器中加入新文件
    : docname: str,文件名
    : nodeID: str,客户端ID
    : address: str,文件服务器地址
    : port: str,文件服务器端口
    : timestamp: str,版本控制时间戳
    '''
    FILE_ADDRESS['docname'] = {"nodeID": nodeID, "address": address, "port": port, "timestamp": timestamp}

def deleteFileMapping(docname):
    '''
    : 从文件路径中删除文件
    : docname: 待删除文件名
    '''
    del FILE_ADDRESS[docname]

def getRandomServer():
    '''
    : 随机选择一个文件服务器
    : return -> 随机一个文件服务器的IP地址和端口号
    '''
    index = random.randint(0, len(FILE_SERVER)-1)
    return FILE_SERVER.items()[index]


class ThreadedHandler(socketserver.BaseRequestHandler):
    '''
    : SocketServer 网络服务框架组件
    : 继承自类socketserver.BaseRequestHandler，该组件协调处理本文件路径服务器与客户端之间的通信，负责处理客户端发来的请求报文
    '''
    def handle(self):
        message = self.request.recv(1024)
        message = json.loads(message)
        requestType = message['request']
        response = ""

        #1. 处理客户端发来的open指令报文
        #-- open报文的处理步骤较为简单，如下所示：
        #-- a. 提取报文中的文件名
        #-- b. 根据报文中的目标文件名，使用文件路径服务器的getFileAddress方法，获得文件具体位置信息（包括文件所在文件服务器地址，文件服务器端口等）
        #-- c. 根据上一步骤中得到的位置信息，生成一个open请求报文，发送给文件服务器以读取其上的文件
        #-- d. 从文件服务器获得响应报文，确认文件已经打开
        if requestType == "open":
            if fileExistsTest(message['docname']):
                fileinfo = getFileAddress(message['docname'])
                response = json.dumps({
                    "response": "open-exists",
                    "docname": message['docname'],
                    "isFile": True,
                    "address": fileinfo['address'],
                    "port": fileinfo['port'],
                    "timestamp": fileinfo['timestamp']
                })
            else:
                fileinfo = getRandomServer()
                response = json.dumps({
                    "response": "open-null",
                    "docname": message['docname'],
                    "isFile": False,
                    "uuid": fileinfo[0],
                    "address": fileinfo[1]['address'],
                    "port": fileinfo[1]['port']
                })

        #2. 处理客户端发来的close指令报文
        #-- close报文的处理步骤较为简单，如下所示：
        #-- a. 提取报文中的文件名
        #-- b. 根据报文中的目标文件名，使用文件路径服务器的getFileAddress方法，获得文件具体位置信息（包括文件所在文件服务器地址，文件服务器端口等）
        #-- c. 根据上一步骤中得到的位置信息，生成一个close请求报文，发送给文件服务器以关闭其上的文件
        #-- d. 从文件服务器获得响应报文，确认文件已经关闭
        elif requestType == "close":
            response = json.dumps({
                "response": "close",
                "docname": message['docname'],
                "isFile": True
            })
        elif requestType == "read":
            if fileExistsTest(message['docname']):
                fileinfo = getFileAddress(docname)
                response = json.dumps({
                    "response": "read-exists",
                    "docname": message['docname'],
                    "isFile": True,
                    "address": fileinfo['address'],
                    "port": fileinfo['port'],
                    "timestamp": fileinfo['timestamp']
                })
            else:
                response = json.dumps({
                    "response": "read-null",
                    "docname": message['docname'],
                    "isFile": False
                }) 

        #3. 处理客户端发来的write指令报文       
        #-- 具体处理步骤和上面的open报文类似，此处不再赘述
        elif requestType == "write":
            print(message['docname'])
            print(FILE_ADDRESS)
            if fileExistsTest(message['docname']):
                print("write if")
                fileinfo = getFileAddress(message['docname'])
                response = json.dumps({
                    "response": "write-exists",
                    "docname": message['docname'],
                    "isFile": True,
                    "uuid": fileinfo['uuid'],
                    "address": fileinfo['address'],
                    "port": fileinfo['port'],
                    "timestamp": message['timestamp']
                })
            else:
                fileinfo = getRandomServer()
                FILE_ADDRESS[message['docname']] = {"uuid": fileinfo[0], "address": fileinfo[1]['address'], "port": fileinfo[1]['port'], "timestamp": message['timestamp']}
                print(FILE_ADDRESS)
                response = json.dumps({
                    "response": "write-null",
                    "docname": message['docname'],
                    "isFile": False,
                    "uuid": fileinfo[0],
                    "address": fileinfo[1]['address'],
                    "port": fileinfo[1]['port'],
                    "timestamp": message['timestamp']
                })
        elif requestType == "dfileinfojoin":
            nodeID = message['uuid']
            if(nodeID == ""):
                nodeID = str(uuid.uuid4())
            FILE_SERVER[nodeID] = {"address": message['address'], "port": message['port']}
            response = json.dumps({"response": requestType, "uuid": nodeID})
            #print(FILE_SERVER)
        else:
            response = json.dumps({"response": "error", "error": requestType+"为非法指令"})

        self.request.sendall(response.encode())


class MasterServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

if __name__ == '__main__':
    address = (ADDRESS, PORT)
    server = MasterServer(address, ThreadedHandler)
    server.serve_forever()