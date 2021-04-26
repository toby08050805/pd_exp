import mysql.connector

class mysqlConnect():
    def __init__(self, ip, port, user, password,db):
        self.ip=ip
        self.port=port
        self.db=db
        self.password=password
        self.user=user
    def getConnectcursor(self):
        try:
            covmo_conn=mysql.connector.connect( host=self.ip , user=self.user, password=self.password, port=self.port, database =self.db)
        except Exception as e:
            print(e)
        return covmo_conn
    def getConnectstring(self):
        return [self.ip, self.password, self.user, self.port, self.db]