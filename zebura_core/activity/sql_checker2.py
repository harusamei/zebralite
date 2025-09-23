# 通过查DB， check SQL 并修正syntax error
#######################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
import logging,asyncio
from zebura_core.utils.conndb1 import connect, db_execute
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.placeholder import check_llm_result, make_a_answ

class CheckSQL:

    def __init__(self, dbServer):

        self.db_name = dbServer['db_name']
        self.db_type = dbServer['db_type']
        self.db_eng = connect(dbServer)
        if self.db_eng is None:
            raise ValueError("Database connection failed")
        
        self.prompter = Prompt_generator()
        self.ans_extr = AnsExtractor()
        self.llm = LLMAgent()

        logging.info("CheckSQL init done")

    # 主函数
    async def check_sql(self, sql, db_schema=None) -> dict:

        for _ in range(2):  # 最多修正1次
            resp = await self.check_once(sql, db_schema)
            if resp['type'] != 'corrected':
                break
            # 修正过，继续检查
            sql = resp['reply']
        return resp
            
    async def check_once(self, sql, db_schema=None):
        # 先通过 explain 检查SQL
        resp = make_a_answ()
        resp['type'] = 'correct' # 本身正确
        resp['reply'] = sql   # 默认不需要修正

        tup = self.explain_sql(sql)
        resp['check_msg'] = tup[1]
        if tup[0]:
            return resp   # SQL正确

        err_msg = tup[1]
        resp['type'] = 'uncorrectable'  # 语法错误，无法修正
        resp['reply'] = ''
        resp['check_msg'] = err_msg
        db_type = self.db_type

        tmpl = self.prompter.tasks['sql_correct']
        query = tmpl.format(sql=sql, db_type=db_type, tb_info=db_schema,error_msg=err_msg)
        llm_aws = await self.llm.ask_llm(query, '')
        result = self.ans_extr.output_extr(llm_aws)
        
        #Track the query
        # outfile = open('tem.out', 'a', encoding='utf-8-sig')
        # outfile.write(query)
        # outfile.write(llm_aws)
        # outfile.write("\n------------\n")

        if not check_llm_result(resp,result):
            return resp
        
        result = result['msg']
        if result.get('type','') == 'uncorrectable':
            return resp
        
        resp['type'] = 'corrected'
        resp['reply'] = result.get('sql','')

        return resp
        
    # 通过运行检查SQL syntax
    def explain_sql(self,sql):
        db_type = self.db_type

        if db_type in ['mysql','postgres']:
            sql_explain = f"EXPLAIN {sql}"
        else:
            return (False, f"{db_type} not supported")
        tup = self.execute_sql(sql_explain)                 # 执行整个SQL
        
        return tup

    def check_sql_result(self, sql):
        tup = self.execute_sql(sql)                 # 执行整个SQL
        if tup[0]:
            row = tup[1].fetchone()
            if row:
                return True
        return False
    
    def is_value_exist(self, tb_name, col, val):

        sql1 = f"SELECT {col} FROM {tb_name} WHERE {col} = '{val}' LIMIT 1"
        sql2 = f"SELECT {col} FROM {tb_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        
        tup = self.execute_sql(sql1)
        if tup[0] and tup[1] > 0:
            return (True, val, 'EXCT')       # (True, val, 'EXCT')
        
        tup = self.execute_sql(sql2)
        if tup[0] and tup[1] > 0:
            return (True, f'%{val}%', 'FUZZY') # (True, val, 'FUZZY')
        return (False, val, 'EMTY')       
        
    # 执行SQL
    def execute_sql(self, sql) -> tuple:
        try:
            result = db_execute(self.db_eng, sql)
            return [True, result]
        except Exception as e:
            return (False, f'{e.args}')

if __name__ == "__main__":

    sqlList = [ "SELECT * FROM books1 WHERE publishdate >= (CURRENT_DATE - INTERVAL 10 YEAR) AND (name LIKE '%神曲%' OR keywords LIKE '%神曲%') AND author != '但丁' UNION SELECT * FROM books2 WHERE publishdate >= (CURRENT_DATE - INTERVAL 10 YEAR) AND (name LIKE '%神曲%' OR keywords LIKE '%神曲%') AND author != '但丁'"]
    
    dbServer = {
            'db_name':'ebook',
            'db_type':'mysql',
            'host':'localhost',
            'port':3306,
            'user':'root',
            'pwd':'zebura'
        }
    # dbServer = {
    #     'db_name':'ebook',
    #     'db_type':'postgres',
    #     'host':'localhost',
    #     'port':5432,
    #     'user':'postgres',
    #     'pwd':'zebura'
    # }
    checker = CheckSQL(dbServer=dbServer)
    for sql in sqlList[:1]:
        print(f"SQL: {sql}")
        rpy = asyncio.run(checker.check_sql(sql))
        print(rpy)
        rpy = checker.check_sql_result(rpy['reply'])
        print(rpy)

    print("done")
