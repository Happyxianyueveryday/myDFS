import socket
import sys
import json
import time
import argparse
import uuid

DIRECTORY_SERVER_ADDRESS = "127.0.0.1"
DIRECTORY_SERVER_PORT = 8080
LOCK_SERVER_ADDRESS = "127.0.0.1"
LOCK_SERVER_PORT = 8888

class Client():
    def __init__(self, directoryAddress, directoryPort, lockAddress, lockPort):
        '''
        : 初始化分布式文件系统客户端
        : directoryAddress: str,文件服务器IP地址
        : directoryPort: str,文件服务器端口
        : lockAddress: str,锁定服务IP地址
        : lockPort: str,锁定服务端口
        '''
        self.id = str(uuid.uuid1())
        self.masterAddr = directoryAddress    #文件服务器IP地址
        self.directoryPort = directoryPort    #文件服务器端口
        self.lockAddr = lockAddress           #锁定服务IP地址
        self.lockPort = lockPort              #锁定服务端口
        self.fileCache = {}                   #客户端文件缓存

    def open(self, docname):
        '''
        : 客户端打开一个文件
        : docname: str,文件名
        '''

        #1. 客户端建立到文件服务器的链接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterAddr, self.directoryPort))

        #2. 客户端发送一个open类型的请求报文给服务器，指示打开指定的文件
        message = json.dumps({"request": "open", "docname": docname, "clientid": self.id})
        sock.sendall(message.encode())
        response = sock.recv(1024)

        return response

    def close(self, docname):
        '''
        : 客户端关闭一个文件
        : docname: str,文件名
        '''

        #1. 客户端建立到文件服务器的链接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterAddr, self.directoryPort))

        #2. 客户端发送一个close类型的请求报文给服务器，指示关闭指定的文件
        message = json.dumps({"request": "close", "docname": docname, "clientid": self.id})
        sock.sendall(message.encode())
        response = sock.recv(2048)
        return response

    def checkLock(self, docname):
        '''
        : 客户端检查一个文件的锁状态
        : docname: str,文件名
        '''

        #1. 客户端建立到锁服务器的链接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.lockAddr, self.lockPort))

        #2. 客户端发送一个checklock类型的请求报文给服务器，指示要获得指定文件的锁的状态
        message = json.dumps({"request": "checklock", "docname": docname, "clientid": self.id})
        sock.sendall(message.encode())
        response = sock.recv(1024)

        return response

    def obtainLock(self, docname):
        '''
        : 为指定的打开的文件获得锁
        : docname: str,文件名
        '''

        #1. 客户端建立到锁服务器的链接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.lockAddr, self.lockPort))

        #2. 客户端发送一个obtainlock类型的请求报文给服务器，指示为指定的文件获得锁
        message = json.dumps({"request": "obtainlock", "docname": docname, "clientid": self.id})
        sock.sendall(message.encode())
        response = sock.recv(1024)

        return response

    def read(self, docname):
        '''
        : 读取指定文件名的文件
        : docname: str,文件名
        '''

        #1. 首先，调用打开文件方法open，该方法将通知路径服务器打开文件，该方法的返回值中包含该文件的具体位置信息
        fileServerInfo = json.loads(self.open(docname))

        #2. 然后，检查open方法的服务器响应报文（返回值）fileServerInfo的'isFile'字段，该字段为True表示目标文件存在，否则目标文件不存在
        if fileServerInfo['isFile']:

            #3. 若fileServerInfo['isFile']==True，即该文件存在，则首先检查该文件是否在缓存中，并且通过timestamp字段检查缓存中的副本是否为最新版本
            if (docname in self.fileCache) and (self.fileCache[docname]['timestamp'] >= fileServerInfo['timestamp']):
                fileCacheFileInfo = self.fileCache[docname]       #3.1 若为最新版本，则直接返回缓存中的目标文件副本即可
                print("Read '" + docname + "' from fileCache!")
                return fileCacheFileInfo
            else:                                          #3.2 若不为最新版本，则根据fileServerInfo的具体位置信息访问文件服务器获得最新版本，并更新缓存中的目标文件副本为最新版本
                addr = fileServerInfo['address']   #从fileServerInfo中获取文件所在的服务器IP地址
                port = int(fileServerInfo['port']) #从fileServerInfo中负责文件I/O的服务器端口

                #4. 客户端再与存有文件的服务器建立连接
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((addr, port))

                #5. 客户端发送一个read类型的请求报文给服务器，指示要从指定文件所在的服务器获得指定的文件的最新版本
                message = json.dumps({"request": "read", "docname": docname, "clientid": self.id})
                sock.sendall(message.encode())

                #6. 使用建立的连接获得最新版本的目标文件，并更新缓存中的副本
                response = sock.recv(1024)

                self.fileCache['docname'] = json.loads(response)

                #7. 返回含有读取文件结果的服务器响应报文
                return response
        else:
        	return docname + "不存在!"

    def write(self, docname, data):
        '''
        : 将更新信息写入文件
        : docname: str,要写入的文件名
        : data: str,要写入的文件的数据
        '''

        #1. 获得需要写入的目标的文件的锁信息lockcheck
        lockcheck = json.loads(client.checkLock(docname))

        #2. 若文件锁信息表项lockcheck['response']="locked"，说明目标文件此时被其他客户端锁定，这时不能写该文件，故直接返回错误信息
        if lockcheck['response'] == "locked":
            return "Cannot write as file is locked by another client!"

        #3. 若文件锁信息表象lockcheck['response']!='locked'，这时目标文件未被其他客户端锁定，这时可以准备写该文件，首先创建到文件所在服务器的链接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterAddr, self.directoryPort))

        #5. 客户端发送一个write类型的请求报文给服务器，指示将要修改指定的文件
        timestamp = time.time()   #生成最新时间戳，作为更新版本号使用

        message = json.dumps({"request": "write", "docname": docname, "clientid": self.id, "timestamp": timestamp})
        sock.sendall(message.encode())

        #6. 客户端受到服务器响应，该响应回送一个报文response，报文中包含目标文件所在的服务器IP地址和端口号
        response = sock.recv(1024)

        fileServerInfo = json.loads(response)

        addr = fileServerInfo['address']
        port = int(fileServerInfo['port'])

        #7. 客户端根据响应报文response的信息，再与存有文件的服务器建立连接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((addr, port))

        #8. 客户端向文件所在的服务器发送write-data请求报文，将需要写入的数据放在该报文中，指示服务器重新写入文件，并更新fileCache中缓存的文件的版本（若没有则在缓存中创建该文件）
        #附注: 需要特别注意，write-data请求报文和write请求报文不相同；write请求报文是发给根结点的，是要请求所要写的文件所在的服务器的IP和端口号；而write-data请求报文是发送给文件所在的服务器的，是要请求该服务器将数据写入指定文件
        content = {"request": "write", "docname": docname, "data": data, "clientid": self.id, "timestamp": timestamp}

        self.fileCache[docname] = content

        message = json.dumps(content)
        sock.sendall(message.encode())     #客户端向文件所在服务器发送write-data请求报文

        response = sock.recv(1024)
        return response


# simple test for the client library
if __name__ == '__main__':
    client = Client(DIRECTORY_SERVER_ADDRESS, DIRECTORY_SERVER_PORT, LOCK_SERVER_ADDRESS, LOCK_SERVER_PORT)

    typeOfCommand = ""
    response = ""

    while typeOfCommand != "exit":
        typeOfCommand = input("请输入一个操作指令[open/close/checklock/read/write]，输入exit以退出:")

        if typeOfCommand == "open":
            docname = str(input("请输入文件名称: "))
            response = client.open(docname)
        elif typeOfCommand == "close":
            docname = str(input("请输入文件名称: "))
            response = client.close(docname)
        elif typeOfCommand == "checklock":
            docname = str(input("请输入文件名称: "))
            response = client.checkLock(docname)
        elif typeOfCommand == "read":
            docname = str(input("请输入文件名称: "))
            response = client.read(docname)
        elif typeOfCommand == "write":
            docname = str(input("请输入文件名称: "))
            data = str(input("请输入要写入的文件内容: "))
            response = client.write(docname, data)
        elif typeOfCommand == "exit":
            response = "成功退出系统!"
        else:
            response = "输入的指令不合法，请重新输入"

        print (response)