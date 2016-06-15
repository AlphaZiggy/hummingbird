#!/usr/bin/env python
#coding: utf-8
'''
Created on 2016/06/01

@author: Alex Luan
'''

import urllib.request
import json
import sys, io
import mysql.connector
import time

API_URL_FMT = ("http://api.map.baidu.com/geocoder/v2/?address={0}&city={1}&output=json&ak={2}")
#Add your key here.
BAIDU_API_KEY = ""
db_user = "spider"
db_pwd = "redips"
db_addr = "127.0.0.1"
db_name = "BusLineDatabase"

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

    def query_ptfm(self):
        QUERY_PTFM_DDL = ("SELECT platform_start FROM " + self.DB_ROAD_STATE_TABLE + " "
                          "UNION SELECT platform_end from " + self.DB_ROAD_STATE_TABLE)
        mysqlCon = self.create_db_connection()
        try:
            cur = mysqlCon.cursor()
            cur.execute(QUERY_PTFM_DDL)
            retPtfm = cur.fetchall()
        except Exception as err:
            print('Unknown PTFM query error!', err)
        mysqlCon.close()
        return retPtfm

    def write_cdt_info(self, ptfm_cdt_list):
        INSERT_CDT_DDL = ("INSERT INTO " + self.DB_PTFM_CDNT_TABLE 
                          + " (platform_name, latitude, longitude) VALUES (\"{0}\", \"{1}\", \"{2}\")")
        mysqlCon = self.create_db_connection()
        cur = mysqlCon.cursor()
        for it in ptfm_cdt_list:
            try:
                cur.execute(INSERT_CDT_DDL.format(it[0], it[1], it[2]))
                print(INSERT_CDT_DDL.format(it[0], it[1], it[2]))
                mysqlCon.commit()
                print("Write CDT info into database: ", it[0], it[1], it[2])
            except Exception as err:
                print('Unknown insert coordinate error!', err)
        mysqlCon.close()

def fetch_coordinate(ptfm_list):
    coordinate_list = []
    if(ptfm_list == None):
        return
    for ptfm in ptfm_list:
        url = API_URL_FMT.format(urllib.parse.quote(ptfm), urllib.parse.quote("苏州市"), BAIDU_API_KEY)
        try:
            resp = urllib.request.urlopen(url)
            result = resp.read().decode()
            jdata = json.loads(result)
            if(jdata["status"] == 0):
                coordinate_list.append([ptfm, jdata["result"]["location"]["lat"], 
                                        jdata["result"]["location"]["lng"]])
                print(ptfm, jdata["result"]["location"]["lat"], jdata["result"]["location"]["lng"])
            else:
                print("Error searching \"{0}\":{1}".format(ptfm, result))
        except Exception as err:
            print("Error when fetching CDT info from Baidu: ", err)
        time.sleep(0.5)
    return coordinate_list   

def run():
    hDB = dbHandler(db_user, db_pwd, db_addr, db_name)
    ptfm_name_list = []
    ptfm_name_tuple = hDB.query_ptfm() 
    for it in ptfm_name_tuple:
        ptfm_name_list.append(it[0])
    ptfm_cdt = fetch_coordinate(ptfm_name_list)
    #ptfm_cdt = fetch_coordinate(["欧尚超市②"])
    #for it in ptfm_cdt:
    #    print(it)
    hDB.write_cdt_info(ptfm_cdt)

if __name__ == "__main__":
    #sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
    run()

