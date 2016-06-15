#!/usr/bin/env python  
# -*- coding:utf-8 -*-  
#  
  
import socket  
import threading  
import socketserver  
import json  
import sys, io
  
def client(ip, port, message):  
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    sock.connect((ip, port))  
  
    try:  
        print("Send: {}".format(message))
        sock.sendall(message.encode(encoding="utf-8"))
        response = sock.recv(10240).decode('utf-8')
        jresp = json.loads(response)  
        print("Recv: ",jresp)
  
    finally:  
        sock.close()  
  
if __name__ == "__main__":  
    # Port 0 means to select an arbitrary unused port  
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
    HOST, PORT = "localhost", 50001 
    #Test register. 
    msg1 = {"register":{"phoneNum":"12345678","userName":"alex","userPwd":"123456"}}
    #Test login.
    msg2 = {"login":{"phoneNum":"12345678","userPwd":"123456"}}
    #msg2 = {'login':{'phoneNum':'13718528992','userPwd':'123456'}}
    msg3 = {"getRoadState":
             {"latitudeMin":"31.782150",
              "latitudeMax":"32.782150",
              "longitudeMin":"120.231186",
              "longitudeMax":"121.231186"
             }
           }
    msg4 = {"uploadTrafficInfo":
             {"phoneNum":"12345678",
              "latitude":"31.282151",
              "longitude":"121.731187",
              "dateTime":"2016-05-14 20:25:31",
              "type":"accident",
              "level":"critical",
              "detail":"a serious traffic accident."
             }
           }
    msg5 = {"getTrafficInfo":
             {"latitudeMin":"30.282150",
              "latitudeMax":"33.282150",
              "longitudeMin":"120.731186",
              "longitudeMax":"123.731186"
             }
           }
    msg6 = {"getUserInfo":{"phoneNum":"12345678"}}
    msg7 = {"getUserInfo":{"phoneNum":"12345678"}}

    jmsg1 = json.dumps(msg1)  
    jmsg2 = json.dumps(msg2)  
    jmsg3 = json.dumps(msg3)  
    jmsg4 = json.dumps(msg4)  
    jmsg5 = json.dumps(msg5)  
    jmsg6 = json.dumps(msg6)
    jmsg7 = json.dumps(msg7);  
  
    #client(HOST, PORT, jmsg1)  
    #client(HOST, PORT, jmsg2)  
    client(HOST, PORT, jmsg3)  
    #client(HOST, PORT, jmsg4)  
    #client(HOST, PORT, jmsg5)  
    #client(HOST, PORT, jmsg6)  
    #client(HOST, PORT, jmsg7)  
