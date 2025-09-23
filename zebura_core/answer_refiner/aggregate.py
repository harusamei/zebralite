# 综合所有解析和查库的信息，最终合成给用户的答案
###################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from zebura_core.placeholder import make_a_answ

class Aggregate:
    def __init__(self):
        pass

    # main func, gathering/combining all info into anawer
    def gathering(self, pipeline) ->dict:
        resp = make_a_answ()
        if not isinstance(pipeline,list) or len(pipeline) == 0:
            resp['status'] = 'failed'
            resp['type'] = 'error'
            resp['reply'] = 'sorry, no query returned'
            return resp
        resp['question'] = pipeline[0].get('question','')      # 用户提问
        resp['reply'] = pipeline[-1].get('reply','')              # 最终回复
        resp['type'] = pipeline[-1].get('type','chat')        # 最终回复类型    
        
        steps_info = []
        issues = []
        for log in pipeline:
            steps_info.append(f'step {log["from"]}, status {log["status"]}, reply: {log.get("reply","")[:50]} ')
            if log['status'] == 'failed':
                issues.append(log['reply'])
            if len(log.get('sql',''))>0:
                steps_info.append(f'  sql: {log["sql"]}')
            if len(log.get('explanation',''))>0:
                steps_info.append(f'  explanation: {log["explanation"]}')
            steps_info.append('-----------------------------------')
        resp['reasoning'] = '\n'.join(steps_info)      # 记录推理过程
        if len(issues)>0:
            resp['issues'] = '\n'.join(issues)
        return resp
     
if __name__ == "__main__":
    answerer = Aggregate()
    answ = answerer.gathering([
        {'from': 'nl2sql', 'status': 'succ', 'reply': 'select * from table1', 'type': 'sql','question':'what is the table1','sql': ''},
        {'from': 'rewrite', 'status': 'succ', 'reply': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'sql_refine', 'status': 'succ', 'reply': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'sql4db', 'status': 'succ', 'reply': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'polish', 'status': 'succ', 'reply': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'sql4db', 'status': 'succ', 'reply': 'select * from table1', 'type': '','question':'what is the table1','sql': ''}
    ])
    print(answ)
