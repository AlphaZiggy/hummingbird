import mysql

class DataHandle():

    def __init__(self):
        self.host = 'localhost'
        self.port = 3306
        self.user = 'root'
        self.pwd = '123456'
        self.db = 'BusLineDatabase'

    def create_db_connection(self):
        try:
            cnx = mysql.connector.connect(self.host,self.port,self.user,self.pwd,self.db)
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return None
        return cnx


    def getBusid(self,cnx): #获得快2线路上所有车的车牌号
        bus = []
        try:
            cur = cnx.cursor()
            cur.execute('select distinct bus_id from BusLineDataTable where line_name like '%快%'')
            bus = cur.fetchall()
            print(bus) #bus 保存车牌号的元组
        except mysql.connector.Error as err:
            print(err.msg)
        cur.close()
        return bus

if __name__ == '__main__':
    handle = DataHandle()
    cnx = handle.create_db_connection()
    bus = handle.geetBusid(cnx)
