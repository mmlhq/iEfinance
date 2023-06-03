# -*- coding: UTF-8 -*-
# 开盘后每30s查询一次概念，对概念涨副超过3%，所属概念个股涨幅小于1%的进行记录（时间、股价、涨幅），
# 按分值排序，*如果有效，后期改钉钉推送，自动买卖

import re
import requests
import json
import random
import pymysql
import time
import baostock

def combine(code):
    match code[:1]:
        case '0' | '3':
            return '0' + code
        case '6':
            return '1' + code
        case '4' | '8':
            return '2' + code
        case _:
            return
def is_chinese(char):
    if '\u4e00' <= char <= '\u9fff':
        return True
    else:
        return False

path = f"C:\\Users\\LiHQ\\AppData\\Local\\VirtualStore\\Program Files\\zd_axzq\\T0002\\blocknew\\"

with open("config/config.json", encoding="utf-8") as f:
    cfg = json.load(f)
info = cfg["mysql"]
cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],
                      database=info["database"])

cur_codes = cnx.cursor()
cur_concept = cnx.cursor()
cur_strongconcept_select = f"SELECT * FROM tdx.concept where strong='Y';"
cur_concept.execute(cur_strongconcept_select)
items = cur_concept.fetchall()
for item in items:
    concept = item[0]
    concept_name = item[1]
    filename = path + concept + ".blk"
    filecfg = path + "blocknew.cfg"
    count = 0  # 概念名中中文字符个数
    for i in concept_name:
        if is_chinese(i):
            count += 1
    with open(filecfg, "ab") as f:
        f.write(concept_name.ljust(50-count, chr(0x0)).encode("gb2312"))  # ljust处理中文时有bug,要减count
        f.write(concept.ljust(70, chr(0x0)).encode("gb2312"))

    with open(filename, "a") as fc:
        cur_concept_codes = f"select i.code from tdx.index i,tdx.score s where locate((select `key` from concept where title = '{concept_name}'),i.concept) and i.code=s.code order by s.total desc limit 39;"
        cur_codes.execute(cur_concept_codes)
        codes = cur_codes.fetchall()
        for icode in codes:
            code = combine(icode[0])+'\n'
            fc.write(code)

cur_codes.close()
cur_concept.close()
cnx.close()
