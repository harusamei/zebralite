########################################233
# 检查sql的可执行性，最终确定可执行的sql
# 一个NL2SQL转换的SQL 需要check 表名，列名，值，条件， revise SQL
############################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from zebura_core.placeholder import make_dbServer, make_a_answ,check_llm_result
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.activity.sql_checker2 import CheckSQL
import logging,asyncio


class GenActivity:

    prompter = Prompt_generator()
    ans_extr = AnsExtractor()
    llm = LLMAgent()
    
    def __init__(self):
       
        serverName = z_config['Training', 'server_name']
        db_name = z_config['Training', 'db_name']
        dbServer = make_dbServer(serverName)
        dbServer['db_name'] = db_name
        
        self.prompter = GenActivity.prompter
        self.ans_extr = GenActivity.ans_extr
        self.llm = GenActivity.llm
        self.scha_loader = ScmaLoader(db_name)
        self.checker = CheckSQL(dbServer)
        
        logging.info("GenActivity init done")

    # 主功能, 生成最终用于查询的SQL, **不管能否生成** reply都是sql
    async def gen_activity(self, question, sql, tb_names=None):

        resp = make_a_answ()
        resp['type'] = 'sql'
        resp['reply'] = sql   # 默认不需要修正
        resp['status'] = 'succ'

        tb_info = self.scha_loader.gen_tbs_prompt(tb_names)
        # 1. check syntax error，correct| corrected| uncorrectable
        result = await self.checker.check_sql(sql,tb_info)
        # 调用LLM出错，与调用后无法修订一样，都是搞不定
        if resp['status'] == 'failed' or result.get('type','uncorrectable') == 'uncorrectable':
            resp['status'] = 'failed'
            resp['type'] = 'err_sqlsyntax'
            resp['reasoning'] = "SQL has syntax error, cannot be corrected"
            resp['err_msg'] = result.get('check_msg','')
            return resp
        # 2. check value error
        resp['type'] = result.get('type','correct') # 默认本身正确，不需要修正
        resp['reply'] = result.get('sql', sql)  # 可能被corrected
        sql = resp['reply']
        # 2.1 有值
        if self.checker.check_sql_result(sql):
            return resp   # SQL正确
        # 2.2 无值，修正value representation mismatch
        result = await self.revise_sql(question, sql, tb_info)
        if result.get('type','uncorrectable') == 'corrected':
            resp['reply'] = result.get('sql', sql)  # 可能被corrected
            resp['type'] = 'corrected'
            resp['reasoning'] += '\n' + result.get('reasoning', '')
        
        return resp
    
    # 修正value representation mismatch
    async def revise_sql(self, question, sql, tb_info=None) -> dict:

        resp = make_a_answ()
        resp['type'] = 'correct' # 本身正确
        resp['sql'] = sql

        tmpl = self.prompter.tasks['sql_revise']
        query = tmpl.format(sql=sql, tb_info=tb_info, question=question)
        llm_aws = await self.llm.ask_llm(query, '')
        result = self.ans_extr.output_extr(llm_aws)
        #Track the query
        # outfile = open('tem.out', 'a', encoding='utf-8-sig')
        # outfile.write(query)
        # outfile.write(llm_aws)
        # outfile.write("\n------------\n")

        # 模型调用出错，姑且认为SQL正确
        if not check_llm_result(resp, result):
            return resp
        
        result = result['msg']
        resp['reasoning'] = result.get('reasoning', '')
        resp['type'] = result.get('type','correct')
        if resp['type'] == 'corrected' and 'sql' in result:
            resp['sql'] = result['sql']
        else:
            resp['err_msg'] = result.get('reasoning','')
        return resp
    
# use example
if __name__ == "__main__":
    gentor = GenActivity()
 
    sql = "SELECT * FROM books WHERE publishdate >= (CURRENT_DATE - INTERVAL '10 YEAR') AND (name LIKE '%神曲%' OR keywords LIKE '%神曲%') AND author != 'Dante'"
    resp = asyncio.run(gentor.gen_activity("近十年书名中有神曲，但作者不是但丁的书", sql))
    print(f"resp:{resp}")
   