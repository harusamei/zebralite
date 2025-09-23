# 解析LLM输出的结果，提取最终信息
# 解析方法遵循json格式,默认LLM输出均为json格式
###########################################
import re
import copy
import json
class AnsExtractor:

    def __init__(self):
        self.result = {'status': 'succ', 'msg': ''}
        # 默认 parse_json
        
    def output_extr(self, llm_answer):
        
        # 存在解析方法
        callFunc = self.parse_json
        result = callFunc(llm_answer)
        if isinstance(result,str):
            result['status'] = 'failed'
            result['msg'] = result
        
        return result
        
    # 解析从LLM获得的SQL提取信息
    def parse_json(self, llm_answer) -> dict:
        result = copy.copy(self.result)

        llm_answer = re.sub(r'[\x00-\x1F]+', ' ', llm_answer)  # 替换控制字符为单个空格
        llm_answer = llm_answer.strip()  # 去除首尾空格

        tlist = llm_answer.split('```json', 1)
        if len(tlist) >1:
            aws = tlist[1]
        else:
            aws = llm_answer
        # 只提取第一个json
        aws = aws.split('```', 1)[0]
        aws = aws.strip()
        aws = re.sub(r',\s*([\]}])', r'\1', aws)    # trailing comma
        
        try:
            # 将 JSON 字符串转换为字典
            aws = aws.replace('\"', '"')
            data = json.loads(aws)
            result['status'] = 'succ'
            result['msg'] = data
        except json.JSONDecodeError as e:
            print(f'Json decode error: {e}\n------------\n')
            result['status'] = 'failed'
            result['msg'] = llm_answer
            print(llm_answer)
        return result

    
if __name__ == "__main__":
    ans_extr = AnsExtractor()
    llm_output = """
\n\n```json\n{\n  "sql": "SELECT * FROM imdb_movie_dataset WHERE director IN (SELECT actors FROM imdb_movie_dataset)",\n  "tables": [\n    {"name": "imdb_movie_dataset", "alias": "t1"}\n  ],\n  "columns": [\n    {"name": "*", "table": "imdb_movie_dataset"}\n  ],\n  "values": []\n}\n```\n\nHowever, the SQL query generated above may not be the most efficient way to solve this problem. A more efficient way would be to use the `IN` operator with a subquery that selects the `actors` column from the same table.\n\nAlternatively, you could use the `EXISTS` operator with a subquery that checks if the `director` exists in the `actors` column.\n\nHere\'s an updated version of the output:\n\n```json\n{\n  "sql": "SELECT * FROM imdb_movie_dataset t1 WHERE EXISTS (SELECT 1 FROM imdb_movie_dataset t2 WHERE t1.director = t2.actors)",\n  "tables": [\n    {"name": "imdb_movie_dataset", "alias": "t1"},\n    {"name": "imdb_movie_dataset", "alias": "t2"}\n  ],\n  "columns": [\n    {"name": "*", "table": "imdb_movie_dataset"}\n  ],\n  "values": []\n}\n```\n\nThis query will return all rows from the `imdb_movie_dataset` table where the `director` exists in the `actors` column.']
                """
    result1 = ans_extr.output_extr(llm_output)
    print(result1)
