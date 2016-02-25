'''
Created on 2015年12月6日

@author: Alex Luan
'''

import urllib.request
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode
import time
from asyncio.tasks import sleep

#快线2号火车站始发
LINE_CID = '175ecd8d-c39d-4116-83ff-109b946d7cb4'
LINE_GUID = '1aa773c8-e865-4847-95a1-f4c956ae02ef'
DB_NAME = 'BusLineDatabase'
DB_ADDR = '192.168.137.128'
DB_USER = 'spider'
DB_PWD = 'spider'
#Table holding bus arriving time of one bus line.
DB_DATA_TABLE = 'KX2HHCZ'
#Table holding platforms of one bus line.
DB_PTFM_TABLE = 'KX2HHCZ_PTFM'
#Time to sleep between two fetch
TIME_INTERVAL = 60

TABLES = {}
TABLES[DB_DATA_TABLE] = (
    "CREATE TABLE IF NOT EXISTS " + DB_DATA_TABLE + '('
    "platform_id CHAR(3) NOT NULL,"
    "bus_id VARCHAR(10) NOT NULL,"
    "arrive_time DATETIME NOT NULL)"
    "ENGINE=InnoDB DEFAULT CHARSET=utf8")

TABLES[DB_PTFM_TABLE] = (
    "CREATE TABLE IF NOT EXISTS " + DB_PTFM_TABLE + '('
    "platform_name VARCHAR(50) NOT NULL,"
    "platform_id CHAR(3) NOT NULL)"
    "ENGINE=InnoDB DEFAULT CHARSET=utf8")

def create_db_tables(tabs, csr):
    for name, ddl in tabs.items():
        try:
            print("Creating table {}: ".format(name), end='')
            #print(ddl)
            csr.execute(ddl)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            print("OK")
    csr.close()
    
def drop_db_tables(csr, db_name):
    DROP_TABLE_DDL = ("DROP TABLE " + db_name)
    try:
        print('Dropping table <' + db_name + '>: ', end='')
        csr.execute(DROP_TABLE_DDL)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")
    
def create_db_connection():         
    try:
        cnx = mysql.connector.connect(user=DB_USER, password=DB_PWD,
                                    host=DB_ADDR,
                                    database=DB_NAME)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None
    return cnx

INSERT_TIME_DDL = ("INSERT INTO " + DB_DATA_TABLE + " "
               "(platform_id, bus_id, arrive_time) "
               "VALUES (%s, %s, %s)")
INSERT_PTFM_DDL = ("INSERT INTO " + DB_PTFM_TABLE + " "
               "(platform_name, platform_id) "
               "VALUES (%s, %s)")
#TODO: Add Error handling.
def insert_db_time_data(cnx, ptfm_id, bus_id, arv_time):
    data_item = (ptfm_id+'', bus_id+'', arv_time)
    #print(type((ptfm_id.string).unicode()))
    #print(ptfm_id.string)
    cnx.cursor().execute(INSERT_TIME_DDL, data_item)
    cnx.commit()

#TODO: Add Error handling.
def insert_db_ptfm_data(cnx, ptfm_name, ptfm_id):
    ptfm_item = (ptfm_name+'', ptfm_id+'')
    cnx.cursor().execute(INSERT_PTFM_DDL, ptfm_item)
    cnx.commit()
    
def fetch_data():
    try:
        text=urllib.request.urlopen('http://www.szjt.gov.cn/BusQuery/APTSLine.aspx?cid=' + LINE_CID +
                                    '&LineGuid=' + LINE_GUID).read()
    except urllib.error.URLError as err:
        if hasattr(err, "reason"):
            print("Failed to reach the server, reason: ", err.reason)
            return None
        elif hasattr(err,"code"):
            print("The server couldn't fulfill the request, error code: ", err.code)
            return None
        else:
            return None
    soup=BeautifulSoup(text , "html.parser")
    table=[]
    line=[]
    for tr in soup.find('table'):
        if (tr.contents[0].contents[0].string == '站台') :
            continue
        tag_a=tr.contents[0]
        station=tag_a.contents[0]
        station_name=station.string
        #print(type(station_name))
        #print(station_name)
        line.append(station_name.string)
        for i in range(1,4):
            td=tr.contents[i]
            #print td.string
            line.append(td.string)
        table.append(line)
        #print(line)
        line=[]
    #print(table)
    return table

def flush_ptfm(con, tab):
    for item in tab:
        print(item)
        insert_db_ptfm_data(con, item[0], item[1])
            
def spider_loop(con, prev):
    prev_data = prev
    while(True):
        time.sleep(TIME_INTERVAL)
        next_data = fetch_data()
        if next_data == None:
            continue
        for i_next in next_data:
            unmoved = False
            if i_next[2] != None:
                for i_prev in prev_data:
                    if i_prev[2] == None:
                        continue
                    if (i_next[1]==i_prev[1] and i_next[2]==i_prev[2] and i_next[3]==i_prev[3]):
                        unmoved = True
                if unmoved == False:
                    insert_db_time_data(con, i_next[1], i_next[2],
                                time.strftime('%Y-%m-%d',time.localtime(time.time())) + ' ' + i_next[3])
        prev_data = next_data
    
#Main function.
cnx = create_db_connection()
print("Connecting mysql server: ", end='')
if cnx != None:
    print('OK')
else:
    print('Failed')
    exit()
drop_db_tables(cnx.cursor(), DB_PTFM_TABLE)
#drop_db_tables(cnx.cursor(), DB_DATA_TABLE)
create_db_tables(TABLES, cnx.cursor())
prev_buf = fetch_data()
if prev_buf == None:
    cnx.close()
    exit()
flush_ptfm(cnx, prev_buf)
print('Spider started...')
spider_loop(cnx, prev_buf)
#cook_data(cnx, prev_buf)
print('Close connection...')
cnx.close()