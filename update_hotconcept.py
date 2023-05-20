# -*- coding: UTF-8 -*-
# 爬取东方财富网的板块信息，写入数据库
import datetime
import re
import requests
import json
import pymysql
import baostock as bs
from apscheduler.schedulers.blocking import BlockingScheduler

def update_concept():
    url = "http://86.push2.eastmoney.com/api/qt/clist/get?"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, "
                             "like Gecko) Chrome/104.0.0.0 Safari/537.36"}
    params = {
        'cb': 'jQuery112409605257827484568_1683038676894',
        'pn': '1',
        'pz': '20',
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

    today = datetime.date.today()

    lg = bs.login()
    rs = bs.query_trade_dates(start_date=today)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    isTradeday = data_list[0][1]
    if isTradeday == "0":
        return

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],
                          database=info["database"])
    cur_concept = cnx.cursor()
    update_sql = "update hotconcept set days = days+1 where (concept, date) in (select concept, date from concept_v);";
    cur_concept.execute(update_sql)

    response = requests.get(url=url, headers=headers, params=params)
    msg = response.content.decode()
    datas = re.findall('jQuery.+\((.+)\)', msg)
    dict_datas = json.loads(datas[0])
    concepts = dict_datas["data"]["diff"]
    for i, concept in enumerate(concepts):
        # 判断是否为热门概念(前10，且涨幅>5%)，写入数据库
        if i > 9:
            break
        gain = concept['f3']
        turn = concept['f8']
        cpt = concept['f12']
        title = concept['f14']
        if gain >= 3 and turn >= 2:
            sql = f"replace into tdx.hotconcept(concept,title,`date`,gain,turn) values('{cpt}','{title}','{today}','{gain}','{turn}');"
            cur_concept.execute(sql)
            cnx.commit()
    cur_concept.close()
    cnx.close()


def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_concept, 'cron', hour=15, minute=8)
    scheduler.start()


dojob()





