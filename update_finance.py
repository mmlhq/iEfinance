# -*- coding: UTF-8 -*-
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


def convert_case(match_obj):
    if match_obj.group(1) is not None:
        return re.sub("'", "`", match_obj.group(1))


def caculate_score(target, value):
    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_level = cnx.cursor()
    select_score_sql = f"select score from level where target ='{target}' and {value}<=high limit 1;"
    cur_level.execute(select_score_sql)
    score = cur_level.fetchone()
    cur_level.close()
    cnx.close()
    return float(score[0])


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
    year = datetime.now().date().year  # 当前年

    quarter = ((month - 1) // 3) + 1
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
            statDate = str(year) + quarterDate[quarter - 1]
            cur_balance_sql = f"select code,statDate from tdx.balance where code='{code}' and statDate='{statDate}';"
            cur_balance.execute(cur_balance_sql)
            findinfo = cur_balance.fetchone()
            if findinfo is None:  # 數據庫中還沒有該數據，寫入數據庫
                rs_balance = bs.query_balance_data(code=code, year=year, quarter=quarter)
                while (rs_balance.error_code == '0') & rs_balance.next():
                    balance_list = rs_balance.get_row_data()
                    dict_b = dict(zip(balance_table_head, balance_list))
                    assetToEquity = 2    #  当查询不到assetToEquity值时，取的缺省值
                    if dict_b['assetToEquity'] != '':
                        assetToEquity = float(dict_b['assetToEquity'])
                    score = caculate_score('balance', assetToEquity)
                    insert_str = re.sub("\[|\]", "",
                                        f"INSERT INTO balance({[k for (k, v) in dict_b.items() if v != '']},'score') " \
                                        f"values{[v for (k, v) in dict_b.items() if v != ''],score};")
                    insert_sql = re.sub(r"(\('.*'\) )", convert_case, insert_str)

                    cur_balance.execute(insert_sql)
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


def update_growth():
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

    quarter = ((month - 1) // 3) + 1
    growth_head = ['code', 'pubDate', 'statDate', 'YOYEquity', 'YOYAsset', 'YOYNI', 'YOYEPSBasic', 'YOYPNI', 'year', 'quarter']
    quarterDate = ['-03-31', '-06-30', '-09-30', '-12-31']

    cur_growth = cnx.cursor()

    times = 3
    while times > 0:  # 向前找3个季度
        quarter -= 1
        if quarter == 0:
            quarter = 4  #
            year -= 1  # 上一年
        for index in tdx_indexs:
            code = combine(index[0])
            statDate = str(year) + quarterDate[quarter - 1]
            cur_growth_sql = f"select code,statDate from tdx.growth where code='{code}' and statDate='{statDate}';"
            cur_growth.execute(cur_growth_sql)
            findinfo = cur_growth.fetchone()
            if findinfo is None:  # 數據庫中還沒有該數據，寫入數據庫
                rs_growth = bs.query_growth_data(code=code, year=year, quarter=quarter)
                while (rs_growth.error_code == '0') & rs_growth.next():
                    growth_list = rs_growth.get_row_data()
                    dict_g = dict(zip(growth_head, growth_list))
                    YOYNI = 0     #  净利润同比增长，当查询不到YOYNI值时，取的缺省值
                    if dict_g['YOYNI'] != '':
                        YOYNI = float(dict_g['YOYNI'])
                    score = caculate_score('growth', YOYNI)
                    insert_str = re.sub("\[|\]", "",
                                        f"INSERT INTO growth({[k for (k, v) in dict_g.items() if v != '']},'score') " \
                                        f"values{[v for (k, v) in dict_g.items() if v != ''],score};")
                    insert_sql = re.sub(r"(\('.*'\) )", convert_case, insert_str)

                    cur_growth.execute(insert_sql)
                    cnx.commit()
                    growth_list.clear()
            else:
                continue
        times -= 1

    cur_index.close()
    cur_growth.close()
    cnx.close()
    # 登出系统
    bs.logout()

def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_balance, 'cron', hour=17, minute=8)
    scheduler.add_job(update_growth, 'cron', hour=18, minute=8)
    scheduler.start()

# dojob()
update_growth()
