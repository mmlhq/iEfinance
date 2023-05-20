# -*- coding: UTF-8 -*-
# 爬取东方财富网的板块信息，写入数据库
import datetime
import re
import requests
import json
import random
import pymysql
import time

def update_index_concept():
    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],
                          database=info["database"])

    url = "https://push2.eastmoney.com/api/qt/clist/get?"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.42"}
    params = {
        'cb': 'jQuery112409605257827484568_1683038676894',
        'pn': '1',
        'pz': '450',
        'po': '1',
        'np': '1',
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': '2',
        'invt': '2',
        'wbp2u': '|0|0|0|web',
        'fid': 'f3',
        'fs': 'm:90 t:3 f:!50',
        'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222',
        '_': '1683038676895'
    }
    response = requests.get(url=url, headers=headers, params=params)
    msg = response.content.decode()
    datas = re.findall('jQuery.+\((.+)\)', msg)
    dict_datas = json.loads(datas[0])
    concepts = dict_datas["data"]["diff"]
    url2 = 'http://push2.eastmoney.com/api/qt/clist/get?'
    params2 = {
        'np': '1',
        'fltt': '1',
        'invt': '2',
        'cb': 'jQuery35104649216960430329_1683821364661',
        'fs': 'b:BK0847',
        'fields': 'f14,f12,f13,f1,f2,f4,f3,f152',
        'fid': 'f3',
        'pn': '1',
        'pz': '1000',
        'po': '1',
        'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
        'wbp2u': '| 0 | 0 | 0 | web',
        '_': '1683821364662'
    }

    millis = int(round(time.time() * 1000))
    expando = "jQuery" + ("3.5.1" + '{:.16f}'.format(random.random())).replace(".", "")
    cb = expando + "_" + str(millis)
    millis = int(round(time.time() * 1000))
    _x = str(millis)
    params2['cb'] = cb
    params2['_'] = _x

    for i, concept in enumerate(concepts):
        BK_concept = concept['f12']
        params2['fs'] = 'b:'+BK_concept
        response2 = requests.get(url=url2, headers=headers, params=params2)
        msg2 = response2.content.decode()
        datas2 = re.findall('jQuery.+\((.+)\)', msg2)
        dict_datas2 = json.loads(datas2[0])
        indexs = dict_datas2["data"]["diff"]
        for index in indexs:
            code = index['f12']
            sql_index_concpets = f"select concept from tdx.index where code='{code}';"
            cur_index_concept = cnx.cursor()
            cur_index_concept.execute(sql_index_concpets)
            _index_concepts = cur_index_concept.fetchall()
            try:
                index_concepts = _index_concepts[0][0]
            except:
                continue
            if index_concepts is None:
                index_concepts = BK_concept
            elif BK_concept not in index_concepts:
                index_concepts = index_concepts + ' ' + BK_concept
            update_concept_sql = f"update tdx.index set concept = '{index_concepts}' where code='{code}';"
            cur_index_concept.execute(update_concept_sql)
        cnx.commit()

    cur_index_concept.close()
    cnx.close()


update_index_concept()



