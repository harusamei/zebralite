# 一些需要保持一致的数据结构
# 一些各处都用的tiny func
############################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config

def make_dbServer(server_name=None):
    if server_name is None:
        dbServer = {
            'db_name':'',
            'db_type':'unknown',
            'host':'localhost',
            'port':1234,
            'user':'totoro',
            'pwd':'123456'
        }
    else:
        dbServer = {
            'db_name': '',               # 数据库名允许未配置
            'db_type':z_config[server_name,'db_type'],
            'host':z_config[server_name,'host'],
            'port':int(z_config[server_name,'port']),
            'user':z_config[server_name,'user'],
            'pwd':z_config[server_name,'pwd']
        }
    return dbServer

# type 含义， 新请求，各类回复，出错，重置，正常状态转移 
#         "type": "query/chat_../err_../reset/transaction", 
#         "status": "new/hold/failed/succ", # 新对话,多轮继续；执行失败；执行成
# action log
def make_a_log(funcName):
        return {                    # sql和question会传递，直至改变
            'sql'      : '',              # 当前步骤产生SQL
            'question' : '',         # 用户提问
            'reply'    : '',            # 当前步骤产生的主要信息, 主要信息sql除外
            'status'   : 'succ',
            'from'     : funcName,       # 当前完成的模块
            'type'     : 'transaction',  # 当前状态类型，默认在状态机中
            'context'  : []           # 上下文信息
        }

def make_a_req(query:str):          # 创建一个请求
    return {
        "msg"   : query,               
        "status": "new",             # new| hold
        "type"  : "request"
    }

def make_a_answ():            # 创建一个回答
    return {'status'    : 'succ',
            'question'  : '',               # 用户提问
            'reasoning' : '',               # 推理过程
            'type'      : 'chat',           # error, chat_xx
            'temp_tbs'  : '',               # SQL查询保存的临时表   TODO
            'reply'     : ''                # 最终回复
    }
# check ans_extractor 处理后的LLM reply 的最基本格式，即有 status 和 msg
def check_llm_result(resp, llm_result):
    flag = True
    if 'status' not in llm_result or 'msg' not in llm_result or llm_result['status'] == 'failed':
        resp['status'] = 'failed'
        resp['reply'] = 'LLM error or answer error'
        resp['type'] = 'err_llm'
        flag = False 
    return flag

def temout(msg):
    if isinstance(msg, list):
        msg = '\n'.join(msg)
    with open('tem.out', 'a', encoding='utf-8-sig') as outfile:
        outfile.write(msg)
        outfile.write("\n------------\n")

# Example usage
if __name__ == '__main__':
    print(make_dbServer())
