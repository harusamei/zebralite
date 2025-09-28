# 执行SQL语句
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from zebura_core.utils.conndb1 import connect, db_execute
import logging
import pandas as pd
from zebura_core.placeholder import make_a_answ
from placeholder import make_dbServer


class ExeActivity:
    def __init__(self):

        serverName = z_config['Training', 'server_name']
        self.db_name = z_config['Training', 'db_name']
        dbServer = make_dbServer(serverName)
        self.db_type = dbServer['db_type']
        dbServer['db_name'] = self.db_name
        self.db_eng = connect(dbServer)
        if self.db_eng is None:
            raise ValueError("Database connection failed")
        
        logging.info("ExeActivity init success")

    
    def checkDB( self, db_name=None ) -> str:  # failed, succ

        if db_name is None:
            db_name = self.db_name
        
        if self.db_type == "mysql":
            sql_query = f"SHOW DATABASES LIKE '{db_name}'"
        elif self.db_type == "postgres":
            sql_query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"
        else:
            print(f"ERR: {self.db_type} not supported")
            return "failed"
        
        result = db_execute(self.db_eng, sql_query)
        if not result:
            print(f"{db_name} not found, create it first")
            return "failed"
        return "succ"
    
    def exeSQL(self, sql):

        answer = make_a_answ()
        try:
            cursor = db_execute(self.db_eng, sql)
            if cursor is None or isinstance(cursor, str):
                answer['status'] = "failed"
                answer['reply'] = f"err_cursor: {cursor}" if isinstance(cursor, str) else "execution failed"
                return answer
            rows = cursor.fetchall() if cursor else []
            columns = cursor.keys() if hasattr(cursor, 'keys') else [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            
            if len(data) > 0:
                answer["reply"] = data
            else:
                answer['reply'] = 'no query results'
        except Exception as e:
            print(f"Error: {e}")
            answer["reply"] = f"err_cursor, {e}"
            answer["status"] = "failed"

        return answer
    
    # database 与 dataframe直接关联
    def sql2df(self, sql):
        try:
            with self.db_eng.connect() as conn:
                df = pd.read_sql_query(
                    sql=sql,
                    con=conn.connection
                )
        except Exception as e:
            print(f"Error: {e}")
            df = pd.DataFrame()
        return df

    def sql2temp(self, sql, temp_tb_name='Temp_table'):
        df = self.sql2df(sql)
        if not df.empty:
            df.to_sql(temp_tb_name, self.db_eng, if_exists='replace', index=False)
            return True
        return False
    
if __name__ == "__main__":
    
    exr = ExeActivity()
    exr.checkDB()
    sql = """
    SELECT * FROM books1 WHERE publisher = '译林出版社' AND (name LIKE '%Dante%' OR keywords LIKE '%Dante%') UNION SELECT * FROM books2 WHERE publisher = '译林出版社' AND (name LIKE '%Dante%' OR keywords LIKE '%Dante%');
"""
    results = exr.exeSQL(sql)
    print(results)
    df = exr.sql2df(sql)
    if not df.empty:
        print(df.columns.to_list())
    
    print("ExeActivity test completed.")
    
