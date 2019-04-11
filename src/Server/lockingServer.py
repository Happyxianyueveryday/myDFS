import socketserver
import json
import os
import sys
import uuid
import random
import time

ADDRESS = "127.0.0.1"
PORT = 8888

LOCK_TIMEOUT = 30

LOCK_LIST = {}     #加锁文件表：加锁文件的"文件名-客户ID"键值对表

def lockExistsTest(docname):
    '''
    : 判断给定文件名的文件是否存在锁
    : docname: 文件名
    '''
    #1. 判断加锁文件表中是否含有该文件名，含有即代表该文件上有锁，否则该文件上无锁
    if docname in LOCK_LIST:
        return True
    else:
        return False

def getLockClient(docname):
    '''
    : 返回给定文件名的文件的锁信息（即加锁的客户ID）
    : docname: 文件名
    '''
    #1. 检查该文件是否含有锁
    if lockExistsTest(docname):
        return LOCK_LIST[docname]
    #2. 若该文件不含有锁，则返回None；否则，返回正在占用（即给文件加锁）的客户ID
    else:
        return None

def addLock(docname, clientid, timestamp, timeout):
    '''
    : 为特定用户给指定文件加锁
    : docname: 文件名
    : clientid: 客户ID
    : timestamp: 时间戳
    : timeout: 加锁最长时限（防止死锁）
    '''
    #1. 在加锁文件表中添加该文件的锁，以及锁的具体信息
    LOCK_LIST[docname] = {"clientid": clientid, "timestamp": timestamp, "timeout": timeout}

def delLock(docname):
    '''
    : 为给指定文件名的文件解锁
    : docname: 文件名
    '''
    #1. 在加锁文件表中删除相关锁的记录以完成解锁
    del LOCK_LIST[docname]

class ThreadedHandler(socketserver.BaseRequestHandler):
    '''
    : SocketServer 网络服务框架组件
    : 继承自类socketserver.BaseRequestHandler，该组件协调处理客户端与锁服务器之间的通信，负责处理客户端发来的请求报文
    : 需要特别注意，锁控制服务是由客户端进行的
    '''
    def handle(self):
        msg = self.request.recv(1024)

        msg = json.loads(msg)
        requestType = msg['request']

        print("Request type = " + requestType)

        response = ""

        if requestType == "checklock":
            if lockExistsTest(msg['docname']):
                print("Check lock -> lock exists")
                timestamp = time.time()
                fs = getLockClient(msg['docname'])


                if fs['timestamp']+fs['timeout'] < timestamp:
                    print("Lock has timed out")
                    print(fs['timestamp']+fs['timeout'])
                    print(timestamp)

                    delLock(msg['docname'])
                    response = json.dumps({
                        "response": "unlocked"
                    })

                elif msg['clientid'] == fs['clientid']:
                    print("Check lock -> lockowned")
                    response = json.dumps({
                        "response": "lockowned",
                        "docname": msg['docname'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })

                else:
                    print("Check lock -> locked")
                    response = json.dumps({
                        "response": "locked",
                        "docname": msg['docname'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })
            else:
                response = json.dumps({
                    "response": "unlocked"
                })

        elif requestType == "obtainlock":
            if lockExistsTest(msg['docname']):
                print("Obtain lock -> lock exists")

                fs = getLockClient(msg['docname'])
                timestamp = time.time()

                if fs['timestamp']+fs['timeout'] < timestamp:
                    print("Obtain lock -> lock timed out, obtain again")

                    print(fs['timestamp']+fs['timeout'])
                    print(timestamp)

                    delLock(msg['docname'])
                    addLock(msg['docname'], msg['clientid'], timestamp, LOCK_TIMEOUT)

                    response = json.dumps({
                        "response": "lockgranted",
                        "docname": msg['docname'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })

                elif msg['clientid'] == fs['clientid']:
                    print("Check lock -> lockowned")
                    timestamp = time.time()
                    response = json.dumps({
                        "response": "lockregranted",
                        "docname": msg['docname'],
                        "timestamp": timestamp,
                        "timeout": LOCK_TIMEOUT
                    })
                else:
                    print("Obtain lock -> locked already")
                    response = json.dumps({
                        "response": "locked",
                        "docname": msg['docname'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })
            else:
                print("Obtain lock -> lock granted")
                timestamp = time.time()
                addLock(msg['docname'], msg['clientid'], timestamp, LOCK_TIMEOUT)

                response = json.dumps({
                    "response": "lockgranted",
                    "docname": msg['docname'],
                    "clientid": msg['clientid'],
                    "timestamp": timestamp,
                    "timeout": LOCK_TIMEOUT
                })
        else:
            response = json.dumps({"response": "Error", "error": requestType+" is not a valid request"})

        self.request.sendall(response.encode())


class LockingServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

if __name__ == '__main__':
    address = (ADDRESS, PORT)
    server = LockingServer(address, ThreadedHandler)
    server.serve_forever()