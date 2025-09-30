# 读取 prompt.txt中的指令模板，用于生成prompt
# prompt文件由base_prompt和lang_prompt组成, base_prompt为默认英文模板，lang_prompt为语言特定的补充模板
############################################
import os,sys
sys.path.insert(0, os.getcwd())
import logging
import re
import zebura_core.constants as const
from settings import z_config
from tabulate import tabulate
from zebura_core.utils.lang_detector import langname2code

# prompt 模板通过文件导入，base prompt文件为当前目录下prompt.txt
# 同时导入role
class Prompt_generator():
    _is_initialized = False

    def __init__(self,lang=None):

        if not Prompt_generator._is_initialized:
            
            Prompt_generator._is_initialized = True
            self.tasks = {}   
            self.roles = {}

            prompt_file = os.path.join(os.getcwd(), const.S_PROMPT_FILE)  # default prompt file
            role_file = os.path.join(os.getcwd(), const.S_ROLE_FILE)      # role file
            lang_prompt = None
            if lang is None:
                lang = z_config['Training', 'chat_lang']
            langcode = langname2code(lang)
            # language special prompt file
            lang_prompt = prompt_file.replace('.txt', f'_{langcode}.txt')  
            if self.load_prompt(prompt_file, lang_prompt):
                logging.debug("Prompt_generator init success")
            else:
                logging.debug("no prompt file, only generate default prompt")
            
            if self.load_role(role_file):
                logging.debug("Role load success")
            else:
                logging.debug("no role file, only generate default role")

            Prompt_generator.tasks = self.tasks
            Prompt_generator.roles = self.roles

    def load_prompt(self, base_prompt, lang_prompt=None):
        loadList = [base_prompt]
        if not os.path.exists(base_prompt):
            print(f"Prompt file {base_prompt} not found")
            return False
        if lang_prompt is not None and os.path.exists(lang_prompt):
            loadList.append(lang_prompt)
        # lang_prompt 补充或更新 prompt_file中的task
        for prompt_file in loadList:
            print(f"Loading prompt from {prompt_file}")
            content = ""
            with open(prompt_file, "r", encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    if line.startswith("//"): # 注释
                        continue
                    if line.startswith("<TASK:"):
                        task_name = line.split(":")[1].strip()
                        task_name = re.sub(r'[^\w]', '', task_name)
                        task_name = task_name.lower()
                        self.tasks[task_name] = "" 
                        content = ""
                    elif line.startswith("</TASK>"):
                        self.tasks[task_name] = content
                    else:
                        content += line
        return True
    
    def load_role(self,role_file):
        print(f"Loading role from {role_file}")
        if not os.path.exists(role_file):
            print(f"Prompt file {role_file} not found")
            return False
        
        content = ""
        with open(role_file, "r", encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith("//"): # 注释
                    continue
                if line.startswith("<ROLE:"):
                    role_name = line.split(":")[1].strip()
                    role_name = re.sub(r'[^\w]', '', role_name)
                    role_name = role_name.lower()
                    self.roles[role_name] = "" 
                    content = ""
                elif line.startswith("</ROLE>"):
                    self.roles[role_name] = content
                else:
                    content += line
        return True
    
    # 得到Prompt
    def get_prompt(self,taskname):
        return self.tasks.get(taskname, f"please do {taskname}")
    
    def get_role(self,rolename):
        return self.roles.get(rolename, f"you are {rolename}")
    
    @staticmethod
    def gen_tabulate(data):
        # 生成简单表格
        return tabulate(data, headers="firstrow", tablefmt="pipe")


# Example usage
if __name__ == '__main__':

    pg = Prompt_generator('japanese')
    print(pg.tasks.keys())
    tmpl =pg.get_prompt('translation')
    print(tmpl)

    print(pg.roles.keys())
    print(pg.get_role('library_assistant'))
    print(pg.get_role('libraryassistant'))


