#!/usr/bin/env python
#coding: utf-8
'''
Created on 2016/05/09

@author: Alex Luan
'''

import socket  
import threading  
import socketserver  
import json, types, string  
import os, time  
import mysql.connector
import sys

db_user = "spider"
db_pwd = "redips"
db_addr = "127.0.0.1"
db_name = "BusLineDatabase"
LOGIN_SUCCEED = 0
LOGIN_WRONG_PWD = -1
LOGIN_USER_NOT_EXIST = -2
LOGIN_OTHER = -3

def daemonize (stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)   #父进程退出  
    except OSError as  e:
        sys.stderr.write ("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror) )
        sys.exit(1)

    #os.chdir("/")  
    os.umask(0)
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)   #第二个父进程退出  
    except OSError as e:
        sys.stderr.write ("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror) )
        sys.exit(1)

    for f in sys.stdout, sys.stderr: f.flush()
    si = open(stdin, 'r')
    so = open(stdout, 'wb+')
    se = open(stderr, 'wb+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())    #dup2函数原子化关闭和复制文件描述符  
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

class dbHandler():
    def __init__(self, db_user, db_pwd, db_addr, db_name):
        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_addr = db_addr
        self.db_name = db_name
        #Save user info.
        self.DB_USER_TABLE = "UserInfo"
        #Save bus stop coordinate.
        self.DB_PTFM_CDNT_TABLE = "PlatformCoordinate"
        #Save road state.
        self.DB_ROAD_STATE_TABLE = "RoadState"
        #Save user reported road state.
        self.DB_USER_REPORTED_TABLE = "UserReportedInfo"

    def create_db_connection(self):
        try:
            cnx = mysql.connector.connect(user=self.db_user, password=self.db_pwd, host=self.db_addr, database=self.db_name)
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return None
        return cnx

    def create_db_tables(self, csr):
        TABLES = {}
        TABLES[db_name] = ("CREATE TABLE IF NOT EXISTS " + self.DB_USER_TABLE + '('
                                 "phone_num VARCHAR(20) NOT NULL PRIMARY KEY,"
                                 "user_name VARCHAR(20) NOT NULL,"
                                 "user_pwd VARCHAR(20) NOT NULL,"
                                 "last_login DATETIME NOT NULL)"
                                 "ENGINE=InnoDB DEFAULT CHARSET=utf8")
        for name, ddl in TABLES.items():
            try:
                print("Creating table {}: ".format(name), end='')
                #print(ddl)
                csr.execute(ddl)
            except mysql.connector.Error as err:
                print(err.msg)
            else:
                print("OK")
        csr.close()

    def register_user(self, phoneNum, userName, userPwd):
        REGISTER_USER_DDL = ("INSERT INTO " + self.DB_USER_TABLE + " "
               "(phone_num, user_name, user_pwd, last_login) "
               "VALUES (%s, %s, %s, %s)")
        registerResult = False
        mysqlCon = self.create_db_connection()
        self.create_db_tables(mysqlCon.cursor())
        mysqlCon.commit()
        try:
            mysqlCon.cursor().execute(REGISTER_USER_DDL, (phoneNum, userName, userPwd, 
                                      time.strftime('%Y-%m-%d',time.localtime(time.time()))))
            mysqlCon.commit()
            registerResult = True
            print('New user registered: ', userName)
        except Exception as err:
            print('New user register error!', err)
        mysqlCon.close()
        return registerResult

    def query_user(self, phoneNum, userPwd):
        QUERY_USER_DDL = ("SELECT user_name, user_pwd FROM " + self.DB_USER_TABLE + " "
                             "WHERE phone_num = {0}")
        loginResult = []
        mysqlCon = self.create_db_connection()
        #self.create_db_tables(mysqlCon.cursor())
        #mysqlCon.commit()
        try:
            cur = mysqlCon.cursor()
            print(QUERY_USER_DDL.format(phoneNum))
            cur.execute(QUERY_USER_DDL.format(phoneNum))
            #retName, retPwd = mysqlCon.cursor().fetchone()
            #TODO: When phone not exist.
            result = cur.fetchone()
            #mysqlCon.commit()
            if(result == None):
                loginResult.append(LOGIN_USER_NOT_EXIST)
            elif(result[1] == userPwd):
                loginResult.append(LOGIN_SUCCEED)
                loginResult.append(result[0])
                print('User query succeed: ', result[0])
            else:
                print('Login wrong password: ', result[0])
                loginResult.append(LOGIN_WRONG_PWD)
        except Exception as err:
            print('Unknown user query error!', err)
            loginResult.append(LOGIN_OTHER)
        mysqlCon.close()
        return loginResult

    def get_user_info(self, phoneNum):
        GET_USER_INFO_DDL = ("SELECT phone_num, user_name FROM " + self.DB_USER_TABLE + " "
                             "WHERE phone_num = \"{0}\"")
        getResult = []
        mysqlCon = self.create_db_connection()
        #self.create_db_tables(mysqlCon.cursor())
        #mysqlCon.commit()
        try:
            cur = mysqlCon.cursor()
            print(GET_USER_INFO_DDL.format(phoneNum))
            cur.execute(GET_USER_INFO_DDL.format(phoneNum))
            getResult = cur.fetchone()
        except Exception as err:
            print('Unknown user info query error!', err)
        mysqlCon.close()
        return getResult

    def add_traffic_info(self, phoneNum, latitude, longitude, address, timestamp, type, level, detail):
        '''
        ("CREATE TABLE IF NOT EXISTS " + self.DB_USER_REPORTED_TABLE + '('
                                  "phone_num VARCHAR(20) NOT NULL PRIMARY KEY,"
                                  "latitude DOUBLE NOT NULL,"
                                  "longitude DOUBLE NOT NULL,"
                                  "address VARCHAR(100) NOT NULL,"
                                  "report_time DATETIME NOT NULL,"
                                  "type VARCHAR(20) NOT NULL,"
                                  "level VARCHAR(20) NOT NULL,"
                                  "detail VARCHAR(200)) "
                                  "ENGINE=InnoDB DEFAULT CHARSET=utf8")
        '''
        ADD_TRAFFIC_INFO_DDL = ("INSERT INTO " + self.DB_USER_REPORTED_TABLE + " "
                                "(phone_num, latitude, longitude, address, report_time, type, level, detail) "
                                "VALUES ({0}, {1}, {2}, \"{3}\", \"{4}\", \"{5}\", \"{6}\", \"{7}\")")
        addResult = False
        mysqlCon = self.create_db_connection()
        #self.create_db_tables(mysqlCon.cursor())
        #mysqlCon.commit()
        try:
            print(ADD_TRAFFIC_INFO_DDL.format(phoneNum, latitude, longitude, address, timestamp, type, level, detail))
            mysqlCon.cursor().execute(ADD_TRAFFIC_INFO_DDL.format(phoneNum, latitude, longitude, 
                                      address, timestamp, type, level, detail)) 
            mysqlCon.commit()
            addResult = True
            print('New reported info added: ', phoneNum)
        except Exception as err:
            print('New reported info add error!', err)
        mysqlCon.close()
        return addResult

    def query_reported_info(self, latiMin, latiMax, longiMin, longiMax):
        QUERY_REPORTED_INFO_DDL = ("SELECT * FROM " + self.DB_USER_REPORTED_TABLE + " "
                                   "WHERE latitude BETWEEN {0} AND {1} "
                                   " AND longitude BETWEEN {2} AND {3}" + " ORDER BY report_time DESC")
        mysqlCon = self.create_db_connection()
        retInfo = []
        try:
            cur = mysqlCon.cursor()
            print(QUERY_REPORTED_INFO_DDL.format(latiMin, latiMax, longiMin, longiMax))
            cur.execute(QUERY_REPORTED_INFO_DDL.format(latiMin, latiMax, longiMin, longiMax))
            #retName, retPwd = mysqlCon.cursor().fetchone()
            retInfo = cur.fetchall()
        except Exception as err:
            print('Unknown reported info query error!', err)
        mysqlCon.close()
        return retInfo

    def query_road_state(self, latiMin, latiMax, longiMin, longiMax):
        '''
        "CREATE TABLE IF NOT EXISTS " + self.DB_PTFM_TABLE + '('
                                  "platform_name VARCHAR(20) NOT NULL PRIMARY KEY,"
                                  "latitude DOUBLE NOT NULL,"
                                  "longtitude DOUBLE NOT NULL)"
                                  "ENGINE=InnoDB DEFAULT CHARSET=utf8"
        '''
        '''
        "INSERT INTO " + self.DB_PTFM_TABLE + " "
               "(platform_name, latitude, longtitude) "
               "VALUES ({0}, {1}, {2})"
        '''
        '''
        "CREATE TABLE IF NOT EXISTS RoadState (
         platform_start VARCHAR(20) NOT NULL, platform_end VARCHAR(20) NOT NULL, state INTEGER NOT NULL) 
         ENGINE=InnoDB DEFAULT CHARSET=utf8"
        '''
        '''
        "INSERT INTO RoadState (platform_start, platform_end, state) VALUES ({0}, {1}, {2})"
        '''
        '''
       "SELECT platform_start, platform_end, state FROM " + self.DB_ROAD_STATE_TABLE + " "
                          "WHERE platform_start OR platform_end IN "
                          "(SELECT platform_name FROM " + self.DB_PTFM_CDNT_TABLE + " "
                          "WHERE latitude BETWEEN {0} AND {1} "
                          " AND longitude BETWEEN {2} AND {3}")
        '''
        QUERY_ROAD_STATE_DDL = ("SELECT platform_start, platform_end, state FROM " + self.DB_ROAD_STATE_TABLE + " "
                               "WHERE platform_start OR platform_end IN "
                               "(SELECT platform_name FROM " + self.DB_PTFM_CDNT_TABLE + " "
                               "WHERE latitude BETWEEN {0} AND {1} "
                               " AND longitude BETWEEN {2} AND {3})")
        mysqlCon = self.create_db_connection()
        try:
            cur = mysqlCon.cursor()
            cur.execute(QUERY_ROAD_STATE_DDL.format(latiMin, latiMax, longiMin, longiMax))
            #retName, retPwd = mysqlCon.cursor().fetchone()
            retState = cur.fetchall()
        except Exception as err:
            print('Unknown road state query error!', err)
        mysqlCon.close()
        return retState

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):  
    def handle(self):
        data = self.request.recv(1024).decode()
        jdata = json.loads(data)  
        print("Receive data from %r" % (data))
        print("Receive jdata from %r" % (jdata))
        if "register" in jdata:
            print(jdata["register"])
            hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
            if(hDB.register_user(jdata["register"]["phoneNum"], jdata["register"]["userName"], jdata["register"]["userPwd"])):
                response = {"registerResult":0}
            else:
                response = {"registerResult":1}
        elif "login" in jdata:
            print(jdata["login"])
            hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
            retName = hDB.query_user(jdata["login"]["phoneNum"], jdata["login"]["userPwd"])
            if(retName[0] == 0):
                response = {"loginResult":0, "userName":retName[1]}
            else:
                response = {"loginResult":retName[0]}
        elif "getUserInfo" in jdata:
            print(jdata["getUserInfo"])
            hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
            retUserInfo = hDB.get_user_info(jdata["getUserInfo"]["phoneNum"])
            response = {"getUserInfo":{}}
            if(retUserInfo != None):
                response["getUserInfo"]["result"] = "0"
                response["getUserInfo"]["phoneNum"] = retUserInfo[0]
                response["getUserInfo"]["userName"] = retUserInfo[1]
            else:
                response["getUserInfo"]["result"] = "1"
        elif "getRoadState" in jdata:
            print(jdata["getRoadState"])
            latitudeMin = jdata["getRoadState"]["latitudeMin"]
            latitudeMax = jdata["getRoadState"]["latitudeMax"]
            longitudeMin = jdata["getRoadState"]["longitudeMin"]
            longitudeMax = jdata["getRoadState"]["longitudeMax"]
            print("Got coodinate info: ", latitudeMin, latitudeMax, longitudeMin, longitudeMax)
            hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
            roadState = hDB.query_road_state(latitudeMin, latitudeMax, longitudeMin, longitudeMax)
            response = {"retRoadState":{"1":[],"2":[],"3":[],"4":[]}}
            for stateItem in roadState:
                response["retRoadState"][str(stateItem[2])].append(stateItem[0] + "-" + stateItem[1])

            '''                
            response = {"retRoadState":{"1":["中科大西-南大研究生院","中科大-独墅湖图书馆"],
                                        "2":["荣域花园-中科大西","独墅湖图书馆-荣域花园"],
                                        "3":["南大研究生院-西交大","西交大-南大研究生院"],
                                        "4":[""]}}
            '''
        elif "getTrafficInfo" in jdata:
            print(jdata["getTrafficInfo"])
            latitudeMin = jdata["getTrafficInfo"]["latitudeMin"]
            latitudeMax = jdata["getTrafficInfo"]["latitudeMax"]
            longitudeMin = jdata["getTrafficInfo"]["longitudeMin"]
            longitudeMax = jdata["getTrafficInfo"]["longitudeMax"]
            print("Got coodinate info: ", latitudeMin, latitudeMax, longitudeMin, longitudeMax)
            hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
            reportedInfo = hDB.query_reported_info(latitudeMin, latitudeMax, longitudeMin, longitudeMax)
            response = {"retTrafficInfo":[]}
            for infoItem in reportedInfo:
                jArrayItem = {"phoneNum":infoItem[0], "latitude":infoItem[1], "longitude":infoItem[2],
                              "address":infoItem[3], "dateTime":infoItem[4].strftime("%Y-%m-%d %H:%M:%S"),
                              "type":infoItem[5], "level":infoItem[6], "detail":infoItem[7]}
                print(jArrayItem)
                response["retTrafficInfo"].append(jArrayItem)
        elif "uploadTrafficInfo" in jdata:
            print(jdata["uploadTrafficInfo"])
            phoneNum = jdata["uploadTrafficInfo"]["phoneNum"]
            latitude = jdata["uploadTrafficInfo"]["latitude"]
            longitude = jdata["uploadTrafficInfo"]["longitude"]
            address = jdata["uploadTrafficInfo"]["address"]
            timestamp = jdata["uploadTrafficInfo"]["dateTime"]
            type = jdata["uploadTrafficInfo"]["type"]
            level = jdata["uploadTrafficInfo"]["level"]
            detail = jdata["uploadTrafficInfo"]["detail"]
            hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
            addResult = hDB.add_traffic_info(phoneNum, latitude, longitude, address, timestamp, type, level, detail)
            if(addResult == True):
                response = {"uploadResult":0, "phoneNum":phoneNum}
            else:
                response = {"uploadResult":1}
        else:
            print("Unknown key!")
            response = {"Unknown CMD":0}

        #cur_thread = threading.current_thread()  
        #response = [{'thread':cur_thread.name,'src':"rec_src",'dst':"rec_dst"}]  

        jresp = json.dumps(response)  
        self.request.sendall(jresp.encode(encoding="utf-8"))
        rec_cmd = "proccess "+'src_val'+" -o "+'dst_val'
        print("CMD %r" % (rec_cmd))
        #os.system(rec_cmd)  
             
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):  
    pass  
  
if __name__ == "__main__":  
    daemonize('/dev/null','/tmp/json_server_stdout.log','/tmp/json_server_error.log')
    # Port 0 means to select an arbitrary unused port  
    HOST, PORT = "0.0.0.0", 50001  
      
    socketserver.TCPServer.allow_reuse_address = True  
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)  
    ip, port = server.server_address  
  
    # Start a thread with the server -- that thread will then start one  
    # more thread for each request  
    server_thread = threading.Thread(target=server.serve_forever)  
  
    # Exit the server thread when the main thread terminates  
    server_thread.aemon = True  
    server_thread.start()  
    print("Server loop running in thread:", server_thread.name)
    print(" .... waiting for connection")
  
    # Activate the server; this will keep running until you  
    # interrupt the program with Ctrl-C  
    server.serve_forever()  
