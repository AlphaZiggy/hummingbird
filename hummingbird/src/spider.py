# coding:utf-8
'''
Created on 2015年12月6日

@author: Alex Luan
'''

import urllib.request
from bs4 import BeautifulSoup
import mysql.connector
import time
import threading
import queue
import configparser

#Time to sleep between two fetch process
TIME_INTERVAL = 60

class writeDatabseThread(threading.Thread):
    def __init__(self, db_user, db_pwd, db_addr, db_name, bus_data_queue):
        threading.Thread.__init__(self)
        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_addr = db_addr
        self.db_name = db_name
        self.bus_data_queue = bus_data_queue
        self.DB_DATA_TABLE = 'BusLineDataTable'
        
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
        TABLES['BusLineDatabase'] = ("CREATE TABLE IF NOT EXISTS " + self.DB_DATA_TABLE + '('
                                 "line_name VARCHAR(50) NOT NULL,"
                                 "platform_name VARCHAR(20) NOT NULL,"
                                 "platform_id CHAR(3) NOT NULL,"
                                 "bus_id VARCHAR(10) NOT NULL,"
                                 "arrive_time DATETIME NOT NULL)"
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
    
    def run(self):
        INSERT_DATA_DDL = ("INSERT INTO " + self.DB_DATA_TABLE + " "
               "(line_name, platform_name, platform_id, bus_id, arrive_time) "
               "VALUES (%s, %s, %s, %s, %s)")
        mysqlCon = self.create_db_connection()
        self.create_db_tables(mysqlCon.cursor())
        mysqlCon.commit()
        while True:
            line, platform, ptfm_id, bus_id, time = self.bus_data_queue.get()
            try:
                mysqlCon.cursor().execute(INSERT_DATA_DDL, (line, platform, ptfm_id, bus_id, time))
                mysqlCon.commit()
                print('Write database:', line, platform, ptfm_id, bus_id, time)
            except Exception as err:
                print('Write database error:', err)
                self.bus_data_queue.put((line, platform, ptfm_id, bus_id, time))
            self.bus_data_queue.task_done()
        mysqlCon.close()
    
class fetchLineInfoThread(threading.Thread):
    #eg. url_tuple = ('快线2号(火车站=>独墅湖高教区首末站)', '175ecd8d-c39d-4116-83ff-109b946d7cb4', '1aa773c8-e865-4847-95a1-f4c956ae02ef')
    #    prev_data = {"快线2号(火车站=>独墅湖高教区首末站)":table[][]}
    def __init__(self, prev_data_dict, line_queue, bus_data_queue):
        threading.Thread.__init__(self)
        self.prev_data_dict = prev_data_dict
        self.line_queue = line_queue
        self.bus_data_queue = bus_data_queue
        
    def fetch_data(self, url_tuple):
        #url = 'http://www.szjt.gov.cn/BusQuery/APTSLine.aspx?cid=' + url_tuple[1] + '&LineGuid=' + url_tuple[2] + '&LineInfo=' + url_tuple[0]
        url = 'http://www.szjt.gov.cn/BusQuery/APTSLine.aspx?cid=' + url_tuple[1] + '&LineGuid=' + url_tuple[2]
        req = urllib.request.Request(url, headers={'user-agent': 'Mozilla/5.0'})
        try:
            html = urllib.request.urlopen(req).read()
        except urllib.error.URLError as err:
            if hasattr(err, "reason"):
                print("Failed to reach the server, reason: ", err.reason)
                return None
            elif hasattr(err,"code"):
                print("The server couldn't fulfill the request, error code: ", err.code)
                return None
            else:
                print('Unknown error while fetching html data:', err)
                return None
        soup = BeautifulSoup(html, "html.parser")
        table = []
        line = []
        if(soup.find('table') == None):
            print('No response from server, wait for next round...')
            return table
        
        for tr in soup.find('table'):
            if (tr.contents[0].contents[0].string == '站台') :
                continue
            tag_a = tr.contents[0]
            station = tag_a.contents[0]
            station_name = station.string
            line.append(station_name.string)
            for i in range(1,4):
                td = tr.contents[i]
                #print td.string
                line.append(td.string)
            table.append(line)
            #print(line)
            line = []
        #print(table)
        return table

    def filter_data(self, next_tab, url_tuple):
        prev_data = self.prev_data_dict[url_tuple[0]]
        next_data = next_tab
        for i_next in next_data:
            unmoved = False
            if i_next[2] != None:
                for i_prev in prev_data:
                    if i_prev[2] == None:
                        continue
                    if (i_next[1]==i_prev[1] and i_next[2]==i_prev[2] and i_next[3]==i_prev[3]):
                        unmoved = True
                if unmoved == False:
                    self.bus_data_queue.put((url_tuple[0], i_next[0]+'', i_next[1]+'', i_next[2]+'',
                                         time.strftime('%Y-%m-%d',time.localtime(time.time())) + ' ' + i_next[3]))
                    print('Put data into queue: ', url_tuple[0], i_next)
        self.prev_data_dict[url_tuple[0]] = next_data
        
    def run(self):
        while True:
            url_tuple = self.line_queue.get()
            self.line_queue.task_done()
            print('Got url tuple from queue: ', url_tuple)
            raw_table = self.fetch_data(url_tuple)
            self.filter_data(raw_table, url_tuple)
            print('Task finished, sleeping...')
            time.sleep(TIME_INTERVAL)
            self.line_queue.put(url_tuple)
    
#Main function.
def work():
    line_queue = queue.Queue(0)
    bus_data_queue = queue.Queue(0)

    conf = configparser.ConfigParser()
    prev_data_dict = {}
    try:
        conf.read('spider.conf')
        db_user = conf.get('BASIC', 'db_username')
        db_pwd = conf.get('BASIC', 'db_password')
        db_addr = conf.get('BASIC', 'db_address')
        db_name = conf.get('BASIC', 'db_name')
        thread_num = conf.get('BASIC', 'work_thread_num')
    except configparser.Error as err:
        print('Miss basic configuration: ', err)
        exit(0)
        
    try:
        for sec in conf:
            if(sec == 'DEFAULT' or sec == 'BASIC'):
                continue
            #print(sec)
            line_name = conf.get(sec, 'name')
            line_cid = conf.get(sec, 'cid')
            line_guid = conf.get(sec, 'guid')
            line_queue.put((line_name, line_cid, line_guid))
            prev_data_dict[line_name] = [] 
    except configparser.Error as err:
        print('Err read line configuration: ', err)
        exit(0)

    if(db_user == None or db_pwd == None or db_addr == None 
       or db_name == None or thread_num == None):
        print('Database configuration error, exit...')
        exit(0)

    w = writeDatabseThread(db_user, db_pwd, db_addr, db_name, bus_data_queue)
    w.setDaemon(True)
    print('Start database daemon')
    w.start()
    
    for i in range(int(thread_num)):
        r = fetchLineInfoThread(prev_data_dict, line_queue, bus_data_queue)
        r.setDaemon(True)
        r.start()
        time.sleep(10)
    
    w.join()
    
if __name__ == '__main__':
    work()
