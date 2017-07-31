import ConFile
import pymysql.cursors
import SQLStatement
class BuildTimeDAO:
    def __init__(self):
        self.dbhost =  ConFile.getConfig('db', 'db_server')
        self.dbname =  ConFile.getConfig('db', 'db_catalog')
        self.dbuser =  ConFile.getConfig('db', 'db_uid')
        self.dbpwd =  ConFile.getConfig('db', 'db_pwd')
        self.__conDB = None
    
    def __ConnectDB(self):
        self.__conDB = pymysql.connect(host = self.dbhost,
                             user =self.dbuser,
                             password = self.dbpwd,
                             db= self.dbname,
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
    
    def __DisConnect(self):
        self.__conDB.close()
    
    def __Commit(self):
        self.__conDB.commit()
        
    def GetAll(self):
        self.__ConnectDB()
        try:
            with self.__conDB.cursor() as cursor:
                sql = SQLStatement.SELECT_ALL_FROM_BUILD_TIME
                cursor.execute(sql)
                return cursor.fetchall()
        finally:
            self.__DisConnect()
            
    def GetJobByJobId(self, job_id):
        try:
            self.__ConnectDB()
            with self.__conDB.cursor() as cursor:
                sql = SQLStatement.GET_JOB_BY_JOB_ID_FROM_BUILD_TIME
                cursor.execute(sql, (job_id))
                return cursor.fetchone()
        finally:
            self.__DisConnect()
    
    def Insert(self, build_job_id, solution, time_cost):
        self.__ConnectDB()
        try:
            with self.__conDB.cursor() as cursor:
                sql = SQLStatement.INSERT_DATA_INTO_BUILD_TIME
                cursor.execute(sql, (build_job_id, solution, time_cost))
            
            self.__Commit()
        finally:
            self.__conDB.close()
    
    def UpdateByJobId(self, job):
        try:
            self.__ConnectDB()
            with self.__conDB.cursor() as cursor:
                sql = SQLStatement.UPDATE_JOB_BY_JOB_ID_FROM_BUILD_TIME
                cursor.execute(sql, (job["solution"], job["time_cost"], job["build_job_id"]))
            
            self.__Commit()
        finally:
            self.__DisConnect()
    
    def DeleteAll(self):
        try:
            self.__ConnectDB()
            with self.__conDB.cursor() as cursor:
                sql = SQLStatement.DELETE_ALL_FROM_BUILD_TIME
                cursor.execute(sql)
                
            self.__Commit()
        finally:
            self.__DisConnect()
        
if(__name__=="__main__"):
    bt = BuildTimeDAO()
    bt.Insert(30, "00-Interfaces.sln.Optimize.x64", "1000")
    bt.Insert(31, "Interfaces.sln.Optimize.Win32", "2000")
    print bt.GetAll()
    
    job={"build_job_id" : 30, "solution" : "update", "time_cost" : "1020"}
    bt.UpdateByJobId(job)
    print bt.GetJobByJobId(30)
    
    bt.DeleteAll()
    print bt.GetAll()
    