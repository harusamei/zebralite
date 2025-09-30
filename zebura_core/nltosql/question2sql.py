#######################################################################################
# main function of nl2sql
# function： natural language question to SQL with LLM
# 3 types of reply: chat / sql / chat_clarify
#######################################################################################
import os,sys,logging
sys.path.insert(0, os.getcwd())
from settings import z_config
import zebura_core.constants as const
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.nltosql.schlinker import Sch_linking
from zebura_core.placeholder import make_a_answ, check_llm_result, temout

class Question2SQL:

    def __init__(self, chat_lang='English'): # 一个用户的database即一个项目

        self.pj_name = z_config['Training', 'db_name']
        self.sys_role = z_config['Training', 'sys_role']   
        self.chat_lang = chat_lang

        self.prompter = Prompt_generator()
        self.llm = LLMAgent()
        self.ans_ext = AnsExtractor()
        self.scha_loader = ScmaLoader(self.pj_name,self.chat_lang)  # 使用默认的 en_prompt
        # 做静态检查，不查数据库
        self.sim_linker = Sch_linking(const.D_SIMILITY_THRESHOLD)

        logging.debug("Question2SQL init success")

    # table_name 相关表名为None, 为整个DB
    # 主函数， 将question转化为SQL
    # {"type": "sql | unclear", "reply".., "tables":[...]}
    async def ques2sql(self, question, tb_names=None) -> dict:

        resp = make_a_answ()
        resp['question'] = question
        # step-1 query analysis
        result = await self.analyze_question(question, tb_names)
        # LLM 出错情况,没通过check
        if not check_llm_result(resp, result):
            return resp

        result = result['msg']  # result['msg'] is a dict
        resp['type'] = 'chat_llm'
        if 'relevance' not in result:
            resp['status'] = 'failed'
            resp['reply'] = 'LLM error or answer error'
            resp['type'] = 'err_llm'
            return resp
        
        if result['relevance'].lower() in ['no','yes and llm only','unclear']:
            resp['status'] = 'succ'
            resp['reply'] = result.get('reply','sorry, I don\'t know')
            if result['relevance'].lower() == 'unclear':
                resp['type'] = 'chat_clarify'    # 需要进一步澄清用户意图
            if result['relevance'].lower() == 'no':
                resp['type'] = 'chat_unrelated'   # 不相关
            return resp
        
        # step-2 nl2sql
        question = result['new_question']   # 前一步修订后的用户提问

        result = await self.nl2sql(question, tb_names)
        if not check_llm_result(resp, result):
            return resp
        
        result = result['msg']  # result['msg'] is a dict
        resp['status'] = 'succ'
        for key in result.keys():
            resp[key] = result[key]
        resp['type'] = 'chat_clarify'       # 默认，需要进一步澄清用户意图
        resp['question'] = question         # 修订后的用户提问
        if 'type' not in result or result['type'].lower() not in ['sql','unclear']:
            resp['status'] = 'failed'
            resp['reply'] = 'LLM error or answer error'
            resp['type'] = 'err_llm'
        elif result['type'].lower() == 'sql':
            resp['type'] = 'sql'
            
        return resp
    
    # lite 版本assume tb较少，不需要选择
    # 分析query 需要DB还是不需要DB,需要DB什么信息
    async def analyze_question(self, question, tb_names=None):

        db_info = self.scha_loader.get_db_info()
        tbs_info = self.scha_loader.gen_tbs_prompt(tb_names)

        tmpl = self.prompter.get_prompt('query_relevance')
        ra = self.prompter.get_role(self.sys_role)
        query = tmpl.format(question=question, db_info=db_info, tbs_info=tbs_info, 
                            sys_role=ra,chat_lang=self.chat_lang)
        llm_answ = await self.llm.ask_llm(query, '')
        # Track the query
        temout([query, llm_answ])
        result = self.ans_ext.output_extr(llm_answ)
        return result
        
    async def nl2sql(self, question, tb_names=None):

        if not isinstance(question,str) or len(question) == 0:
            return {'status': 'failed', 'msg': 'question is empty'}
        
        tbs_info = self.scha_loader.gen_tbs_prompt(tb_names)
        tmpl = self.prompter.get_prompt('nl_to_sql')
        query = tmpl.format(tbs_info=tbs_info, question=question,chat_lang=self.chat_lang)
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr(llm_answ)
        # Track the query
        with open('tem.out', 'a', encoding='utf-8-sig') as outfile:
            outfile.write(query)
            outfile.write(llm_answ)
            outfile.write("\n------------\n")
        return result
        
    def get_tables_info(self, tb_names) -> str:
        return self.scha_loader.gen_tbs_prompt(tb_names)
        
# Example usage
if __name__ == '__main__':
    import asyncio

    querys = ['収益が最も高い映画は何ですか？','2000年以后有多少本跟心理学相关的书',
              '你知道如何查询图书的价格吗？','近年来出版了多少本康德写的书']
    table_names = ['books1','books2']
    chat_lang = z_config['Training', 'chat_lang']
    parser = Question2SQL(chat_lang=chat_lang)
    for query in querys[1:]:
        result = asyncio.run(parser.ques2sql(query,tb_names=table_names))
        print(result)

    print('Done')