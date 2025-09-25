###########################################
# 与chatbot交互的接口, 内部是一个总控制器，负责调度各个模块最终完成DB查询，返回结果
############################################
import sys,os, time
sys.path.insert(0, os.getcwd().lower())

from settings import z_config
import pandas as pd
import logging,asyncio,inspect

from zebura_core.nltosql.question2sql import Question2SQL
from zebura_core.activity.exe_activity1 import ExeActivity
from zebura_core.activity.gen_activity1 import GenActivity
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.placeholder import make_a_log, make_a_req,make_a_answ,check_llm_result,temout

# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束
class Controller:   
    chat_lang = z_config['Training', 'chat_lang']

    parser = Question2SQL(chat_lang=chat_lang)         # nl2SQL
    act_maker = GenActivity()       # gen exectuable SQL
    executor = ExeActivity()        # query sql from DB
    llm = LLMAgent()
    
    def __init__(self):

        self.prompter = Controller.parser.prompter  # prompt generator
        self.rel_tbnames = []
        self.endTypes = ["end","chat","err"]
        self.stations = ['nl2sql', 'rewrite', 'sql_refine', 'db_enhance','sql4db', 
                         'end', 'transit']
        # 状态出错时的默认回复
        self.default_reply = {
                    "any": "something went wrong, please try again.",
                    "nl2sql": "sorry, I cannot understand your question, could you please rephrase it?",
                    "sql4db": "sorry, I cannot execute the SQL query, please check the syntax and try again."
        }
        
        # 默认的执行策略                             # related table names
        self.matrix = {
            "(new,user)": self.nl2sql,
            "(hold,user)": self.rewrite,            # rewrite question when multi-turn
            "(succ,rewrite)": self.nl2sql,          
            "(succ,nl2sql)": self.sql_refine,       # check sql before query db
            "(succ,sql4db)": self.db_enhance,       # db-augmented generation
            "(succ,db_enhance)": self.end,          
            "(succ,sql_refine)": self.sql4db,       # query db
            "(failed,transit)": self.end,           # end
            "(failed,*)": self.transit,             # reset action
            "(*,*)": self.end                       # whitelist principle: end anything not on the list
        }
        logging.info("Controller init success, the current chat language is %s" % self.chat_lang)
    
    def isEnd(self,log):
        for state in self.endTypes:
            if state in log.get('type','').lower():
                return True
        return False
    
    def check_result(self,log, result, keys=[]):
        flag = check_llm_result(log, result)
        if not flag:
            return False
        for key in keys:
            if key not in result.get('msg', {}):
                log['status'] = 'failed'
                log['type'] = 'err_llm'
                log['reply'] = self.default_reply['any']
                return False
        return True

    def get_next(self, pipeline):
        
        lastLog = pipeline[-1]
        # chat or error 自动机结束
        if self.isEnd(lastLog):
            return self.end
        
        nextStep = self.end
        # 强制指定跳转
        if lastLog['type'] == "reset":
            method = getattr(self, lastLog['from'])
            lastLog['from']='transit'       # 恢复之前状态机转移时的占用
            nextStep = method

        curSt = f'({lastLog["status"]},{lastLog["from"]})'
        if curSt in self.matrix:
            nextStep = self.matrix[curSt]
        else:
            curSt = f'({lastLog["status"]},*)'
            if curSt in self.matrix:
                nextStep = self.matrix[curSt]
            else:
                curSt = '(*,*)'
                if curSt in self.matrix:
                    nextStep = self.matrix[curSt]
        
        if nextStep != self.end:
            log = make_a_log(nextStep.__name__)
            log['question'] = lastLog['question']   # 默认传递question,sql
            log['sql'] = lastLog['sql']
            pipeline.append(log)
        
        if len(pipeline) < 2:
            nextStep = self.end

        # print('step:', nextStep.__name__)
        return nextStep
    
    # 状态执行失败后应做的一些处理，决定下一步怎么走
    def transit(self, pipeline):
        # 默认转移到最后一个状态
        new_log = pipeline[-1]
        new_log['type'] = "reset"
        new_log['reply'] = self.default_reply.get(new_log['from'], self.default_reply['any'])
        
        fromList = [log['from'] for log in pipeline]
        frm = fromList[-1]
        
        # 不在状态机中，直接结束
        if frm not in self.stations:
            new_log['from'] = 'end'
            return

        # 开局即失败，直接结束
        if frm in ['nl2sql','rewrite']:  # 处理 nl2sql 发起的跳转
            new_log['from'] = 'end'
            return   
        
        if frm == 'sql_refine':  # sql不能执行，只能end
            new_log['from'] = 'end'
            return
        
        # 兜底转移到最后一个状态
        new_log['from'] = 'end'
        return

    # question to sql
    async def nl2sql(self, pipeline):

        new_log, pre_log = pipeline[-1], pipeline[-2]
        answ = await self.parser.ques2sql(pre_log['question'],tb_names=self.rel_tbnames)
        new_log = self.copy_to_log(answ, new_log)
        new_log['question'] = answ.get('question', pre_log['question'])
        if answ.get('type','') == 'sql':  # sql or unclear
            new_log['sql'] = answ['reply']


    # 查询DB前修正语法错误，生成一个可执行的SQL，但不保证有查询结果
    async def sql_refine(self, pipeline):

        new_log, pre_log = pipeline[-1], pipeline[-2]
        question = pipeline[0]['question']      # 用户的原始提问
        sql = pre_log['sql']
        tb_names = pre_log.get('tables',None)   # sql中包含的表名列表，optional

        answ = await self.act_maker.gen_activity(question,sql,tb_names)
        
        new_log = self.copy_to_log(answ, new_log)
        new_log['sql'] = answ['reply']

        return

    # multi-turn, 带有上下文时从这里开始
    async def rewrite(self, pipeline):

        new_log = pipeline[-1]
        history = pipeline[0]['context'] if 'context' in pipeline[0] else []
        # 保留最近3轮的请求
        msgs = []
        for contx in history[-6:]:
            if 'request' in contx['type']:
                msg = f"user: {contx.get('msg')}"
            else:
                msg = f"agent: {contx.get('reply')}"
            msgs.append(msg)
        history = '\n'.join(msgs)
        last_query = new_log['question']
        chat_lang = self.chat_lang

        tmpl = self.prompter.get_prompt('rewrite')
        query = tmpl.format(history=history, last_query=last_query, chat_lang=chat_lang)
        llm_answ = await self.llm.ask_llm(query, "")
        # Track the query
        temout([query, llm_answ])

        result = self.act_maker.ans_extr.output_extr(llm_answ)
        if not self.check_result(new_log, result, ['intent','rewritten']):
            return
        result = result['msg']
        if result['intent'].lower() == 'end':
            new_log['status'] = 'succ'
            new_log['reply'] = 'thanks, let\'s end the conversation.'
            new_log['type'] = 'end'
        elif result['intent'].lower() == 'confirm':
            new_log['status'] = 'succ'
            new_log['reply'] = result.get('direct_reply','Sure, I am here to help you.')
            new_log['type'] = 'chat_confirm'
        else: 
            new_log['status'] = 'succ'
            new_log['ori_question'] = new_log['question']
            new_log['question'] = result['rewritten']
            new_log['reply'] = result['rewritten']
        return

    # query db
    def sql4db(self, pipeline):
        new_log = pipeline[-1]
        sql = new_log['sql']
        answ = self.executor.exeSQL(sql)
        if answ['status'] == 'failed':
            new_log['status'] = 'failed'
            new_log['type'] = 'err_sql'
            new_log['err_msg'] = answ.get('reply','')
            new_log['reply'] = self.default_reply.get('sql4db', self.default_reply['any'])
            return
        
        new_log = self.copy_to_log(answ, new_log)
        new_log['type'] = 'db_result'

    # data augmented generation
    async def db_enhance(self, pipeline):

        new_log, pre_log = pipeline[-1], pipeline[-2]
        sql = new_log['sql']
        question = pipeline[0]['question']
        new_log['question'] = question

        db_info = self.parser.scha_loader.get_db_info()
        max_rows = 20
        result = pre_log.get('reply',[])
        sql_result = 'no result found\n'
        Temp_table = "no_table"
        if len(result) > 0:
            sql_result = 'total rows count: {}\n'.format(len(result))        
            tdf = pd.DataFrame(result[:max_rows])
            sql_result += tdf.to_markdown(index=False, tablefmt="grid")
            Temp_table = f"temp_table{len(pipeline)}"
        
        tmpl = self.prompter.get_prompt('db_enhance')
        
        query = tmpl.format(chat_lang=self.chat_lang, sql=sql, question=question, db_info=db_info, 
                            sql_result=sql_result, Temp_table=Temp_table)
        llm_answ = await self.llm.ask_llm(query, "")
        # Track the query
        temout([query, llm_answ])

        result = self.act_maker.ans_extr.output_extr(llm_answ)
        if result['status'] == 'succ' and 'reply' in result.get('msg',{}):
            new_log = self.copy_to_log(result.get('msg',{}), new_log)
            new_log['type']='chat_db'
        else:
            new_log['status'] = 'failed'
            new_log['type'] = 'err_llm'
        
        return
    
    # end of the state machine， 啥也不干
    def end(self, pipeline):
        # for log in pipeline:
        #     print(f"step:{log['from']}, status:{log['status']}")
        # print(pipeline[-1]['msg'])
        return "end"
    
    # 出错情况的补救
    def genAnswer(self, pipeline) -> dict:
        resp = make_a_answ()
        resp['type'] = pipeline[-1].get('type','chat')              # 最终回复类型
        resp['reply'] = pipeline[-1].get('reply','')                # 最终回复 
        resp['sql'] = pipeline[-1].get('sql','')
        
        answ = []
        for log in pipeline:
            answ.append(f'step {log["from"]}, status {log["status"]}, reply: {log.get("reply","")[:50]} ')
            if log.get('from') == 'nl2sql':
                resp['explanation'] = log.get('explanation','')
        resp['reasoning'] = '\n'.join(answ)      # 记录推理过程
        last_log = pipeline[-1]
        if last_log['from'] in ['sql_refine','sql4db'] and last_log['status'] == 'failed':
            temStr = f'user question: {pipeline[0]["question"]}\n'
            temStr += f'sql: {last_log.get("sql","")}\n'
            temStr += f'error msg: {last_log.get("err_msg","")}\n'
            # TODO , 可以用LLM润色
            resp['reply'] = '转换为SQL语句出错了，信息如下：\n'+temStr
        return resp

     # 当前问题涉及的表名
    def set_rel_tbnames(self, tbnames: list):
        self.rel_tbnames = tbnames
    
    def copy_to_log(self, result, new_log):
        keys_to_copy = result.keys() - {'from','sql','question'}
        for k in keys_to_copy:
            new_log[k] = result[k]
        return new_log


# 主控流程，负责调用不同的funcs
async def apply(request, context):
    controller = Controller()
    # 记录所有状态，包括transit，不删除任何状态
    # pipeline 中记录 question, sql的变化
    pipeline = list()
    new_log = make_a_log("user")
    new_log = controller.copy_to_log(request, new_log)
    new_log['question'] = request['msg']
    new_log['context'] = context
    pipeline.append(new_log)

    nextStep = controller.get_next(pipeline)
    while nextStep != controller.end:
        if inspect.iscoroutinefunction(nextStep):
            await nextStep(pipeline)
        else:
            nextStep(pipeline)
        nextStep = controller.get_next(pipeline)
    controller.end(pipeline)
    resp = controller.genAnswer(pipeline)
    return resp

async def main():
    
    questions = ['近年来出版了多少本康德写的书',
                 '作者名包含但丁的书',
                 '作者名含但丁的书一共有多少本',
                'List the books related to The Divine Comedy published in the last ten years that do not include Dante as an author',
                '请告诉我盗梦空间的详细信息',
                '这是一部什么类型的电影',
                'How many movies in the dataset have a revenue greater than 100 million dollars?',
                'What is the average metascore of the movies in the dataset?',
                '列出可以抽烟的餐厅']
    user_msgs = [
        "最近十年出版了多少本神曲有关的书",
        "你只查询了一张数据库表吗",
        "你查询数据库的所有表，告诉我结果",
        "列出其中作者不包含但丁的书"
    ]
    user_msgs = ['有多少本跟神曲相关的书']
    context = list()
    for i, msg in enumerate(user_msgs):
        start = time.time()
        request = make_a_req(msg)
        if i > 0:
           request['status'] = 'hold'
        print(f"=============\nQuestion: {msg}")
        resp = await apply(request,context)

        context.append(request)
        context.append(resp)

        print(f"Time: {int(time.time()-start)}")
        for k,v in resp.items():
            if v is not None and len(str(v)) > 0:
                print(f"{k}: {v}")
        print(f"Answer: {resp['status']}")
        print(f"{resp['reply']}\n")
        print(f"{resp['reasoning']}")
        print("=============")
    
if __name__ == "__main__":
    asyncio.run(main())
    print("Done")
    