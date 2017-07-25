import pymysql.cursors

class DBInfo:
    def __init__(self):
        self.host = 'db_host'
        self.user='db_user'
        self.password='db_password'
        self.db='db_name'

class PythonMySQL:
    def __init__(self, dbInfo):
        self.dbInfo = dbInfo
        self.__conDB = None
    
    def __ConnectDB(self):
        self.__conDB = pymysql.connect(host = self.dbInfo.host,
                             user =self.dbInfo.user,
                             password = self.dbInfo.password,
                             db= self.dbInfo.db,
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
    
    def __DisConnect(self):
            self.__conDB.close()
    
    def Query(self, table, target='*', criterion=None):
        self.__ConnectDB()
        try:
            with self.__conDB.cursor() as cursor:
            # Read a single record
                if(criterion):
                    cursor.execute("SELECT {0} FROM {1} WHERE {2}".format(target, table, criterion))
                else:
                    cursor.execute("SELECT {0} FROM {1}".format(target, table))
                return cursor.fetchall()
        finally:
            self.__DisConnect()
      
    def ExecutrCustomSQL(self, sqlString):
        self.__ConnectDB()
        try:
            with self.conDB.cursor() as cursor:
                # Read a single record
                cursor.execute(sqlString)
                return cursor.fetchall()
        finally:
            self.__DisConnect()
