#-*- coding: UTF-8 -*-
# 更新股票中tdx.balance表信息

import baostock as bs
import pymysql
import re
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import json


def combine(code):
    match code[:1]:
        case '0' | '3':
            return 'sz.' + code
        case '6':
            return 'sh.' + code
        case '4' | '8':
            return 'bj.' + code
        case _:
            return


def update_balance():
    # 登陆网站系统
    lg = bs.login()

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_index = cnx.cursor()
    cur_index_sql = "select code from tdx.index;"
    cur_index.execute(cur_index_sql)
    tdx_indexs = cur_index.fetchall()
    month = datetime.now().date().month  # 当前月
    year = datetime.now().date().year    # 当前年

    quarter = ((month - 1) // 3)+1
    balance_table_head = ['code', 'pubDate', 'statDate', 'currentRatio', 'quickRatio', 'cashRatio', 'YOYLiability',
                    'liabilityToAsset', 'assetToEquity']
    quarterDate = ['-03-31', '-06-30', '-09-30', '-12-31']

    cur_balance = cnx.cursor()

    times = 3
    while times > 0:  # 向前找3个季度
        quarter -= 1
        if quarter == 0:
            quarter = 4  #
            year -= 1  # 上一年
        for index in tdx_indexs:
            code = combine(index[0])
            statDate = str(year) + quarterDate[quarter-1]
            cur_balance_sql = f"select code,statDate from tdx.balance where code='{code}' and statDate='{statDate}';"
            cur_balance.execute(cur_balance_sql)
            findinfo = cur_balance.fetchone()
            if findinfo is None:  # 數據庫中還沒有該數據，寫入數據庫
                rs_balance = bs.query_balance_data(code=code, year=year, quarter=quarter)
                while (rs_balance.error_code == '0') & rs_balance.next():
                    balance_list = rs_balance.get_row_data()
                    d1 = zip(balance_table_head, balance_list)
                    mydict = dict(d1)
                    insert_sql = "INSERT INTO balance("
                    value_sql = " VALUES("
                    value_list = []
                    for bitem in range(len(balance_list)):
                        if balance_list[bitem] != '':
                            insert_sql += balance_table_head[bitem]+','
                            value_sql += "'%s',"
                            value_list.append(balance_list[bitem])
                    cur_balance_insert = re.sub(',$', ')', insert_sql) + re.sub(',$', ')', value_sql)
                    cur_balance.execute(cur_balance_insert % tuple(value_list))
                    cnx.commit()
                    balance_list.clear()
            else:
                continue
        times -= 1

    cur_index.close()
    cur_balance.close()
    cnx.close()
    # 登出系统
    bs.logout()

# def dojob():
#     scheduler = BlockingScheduler()
#     scheduler.add_job(update_balance, 'cron', hour=16, minute=18)
#     scheduler.start()

# dojob()


update_balance()