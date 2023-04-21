# -*- coding: UTF-8 -*-
# 更新股票中tdx.balance表信息

import baostock as bs
import pymysql
import re
from datetime import datetime
import pandas as pd
import numpy as np
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


# def caculate_score(target, value):
#     with open("config/config.json", encoding="utf-8") as f:
#         cfg = json.load(f)
#     info = cfg["mysql"]
#     cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
#     cur_level = cnx.cursor()
#     select_score_sql = f"select score from level where target ='{target}' and {value}<=high limit 1;"
#     cur_level.execute(select_score_sql)
#     score = cur_level.fetchone()
#     cur_level.close()
#     cnx.close()
#     return float(score[0])

def caculate_score(target, value, pd_level):
    pd_score = pd_level[(pd_level['target']==target) & (pd_level['high']>=value) & (pd_level['low']<value)]
    if pd_score.empty:
        return 0
    score = pd_score.iloc[0,4]
    return float(score)


def leveltable_to_df(cnx):
    sql = "select * from tdx.level"
    df = pd.io.sql.read_sql_query(sql,cnx)
    return df


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
    pd_level = leveltable_to_df(cnx)

    cur_balance = cnx.cursor()
    times = 3
    while times > 0:  # 向前找3个季度
        quarter -= 1
        if quarter == 0:
            quarter = 4  #
            year -= 1  # 上一年
        item_list = []
        for index in tdx_indexs:
            code = combine(index[0])
            statDate = str(year) + quarterDate[quarter - 1]

            rs_balance = bs.query_balance_data(code=code, year=year, quarter=quarter)
            while (rs_balance.error_code == '0') & rs_balance.next():
                balance_list = rs_balance.get_row_data()
                dict_b = dict(zip(balance_table_head, balance_list))
                if dict_b['assetToEquity'] != '':
                    assetToEquity = float(dict_b['assetToEquity'])
                score = caculate_score('balance', assetToEquity, pd_level)
                s = str(balance_list).replace('[', '').replace(']', '').replace("''", 'null')
                insert_str = "REPLACE INTO balance(`code`, `pubDate`, `statDate`, `currentRatio`, `quickRatio`, `cashRatio`, " \
                             f"`YOYLiability`,`liabilityToAsset`, `assetToEquity`,`score`) VALUES({s},{score});"
                cur_balance.execute(insert_str)
                cnx.commit()
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
    growth_head = ['code', 'pubDate', 'statDate', 'YOYEquity', 'YOYAsset', 'YOYNI', 'YOYEPSBasic', 'YOYPNI']
    quarterDate = ['-03-31', '-06-30', '-09-30', '-12-31']

    cur_growth = cnx.cursor()
    pd_level = leveltable_to_df(cnx)
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
                    score = caculate_score('growth', YOYNI, pd_level)
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


def update_profit():
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
    profit_head = ['code','pubDate','statDate','roeAvg','npMargin','gpMargin','netProfit','epsTTM','MBRevenue','totalShare','liqaShare']
    quarterDate = ['-03-31', '-06-30', '-09-30', '-12-31']

    cur_profit = cnx.cursor()
    pd_level = leveltable_to_df(cnx)
    times = 3
    while times > 0:  # 向前找3个季度
        quarter -= 1
        if quarter == 0:
            quarter = 4  #
            year -= 1  # 上一年
        for index in tdx_indexs:
            code = combine(index[0])
            statDate = str(year) + quarterDate[quarter - 1]
            cur_profit_sql = f"select code,statDate from tdx.profit where code='{code}' and statDate='{statDate}';"
            cur_profit.execute(cur_profit_sql)
            findinfo = cur_profit.fetchone()
            if findinfo is None:  # 數據庫中還沒有該數據，寫入數據庫
                rs_profit = bs.query_profit_data(code=code, year=year, quarter=quarter)
                while (rs_profit.error_code == '0') & rs_profit.next():
                    profit_list = rs_profit.get_row_data()
                    dict_g = dict(zip(profit_head, profit_list))
                    roeAvg = 0.08     #  净资产收益率，当查询不到roeAvg值时，取的缺省值
                    if dict_g['roeAvg'] != '':
                        roeAvg = float(dict_g['roeAvg'])
                    score = caculate_score('profit', roeAvg, pd_level)
                    insert_str = re.sub("\[|\]", "",
                                        f"INSERT INTO profit({[k for (k, v) in dict_g.items() if v != '']},'score') " \
                                        f"values{[v for (k, v) in dict_g.items() if v != ''],score};")
                    insert_sql = re.sub(r"(\('.*'\) )", convert_case, insert_str)

                    cur_profit.execute(insert_sql)
                    cnx.commit()
                    profit_list.clear()
            else:
                continue
        times -= 1

    cur_index.close()
    cur_profit.close()
    cnx.close()
    # 登出系统
    bs.logout()


def update_score():
    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    date = datetime.today().date()
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_balance = cnx.cursor()
    cur_score = cnx.cursor()
    balance_sql = "select b.code,b.score from tdx.balance b where (b.`code`,b.statDate) in (select `code`,max(statDate) from tdx.balance group by `code`);"
    cur_balance.execute(balance_sql)
    balance_scores = cur_balance.fetchall()
    for item in balance_scores:
        code = item[0][3::]
        score = item[1]
        bdate = datetime.today().date()
        update_sql = f"UPDATE `tdx`.`score` SET balance='{score}',`date`='{bdate}' where `code`='{code}'; "
        cur_score.execute(update_sql)
        cnx.commit()

    cur_growth = cnx.cursor()
    growth_sql = "select b.code,b.score from tdx.growth b where (b.`code`,b.statDate) in (select `code`,max(statDate) from tdx.growth group by `code`);"
    cur_growth.execute(growth_sql)
    growth_scores = cur_growth.fetchall()
    for item in growth_scores:
        code = item[0][3::]
        score = item[1]
        gdate = datetime.today().date()
        update_sql = f"UPDATE `tdx`.`score` SET growth='{score}', `date`='{gdate}' where `code`='{code}'; "
        cur_score.execute(update_sql)
        cnx.commit()

    cur_profit = cnx.cursor()
    profit_sql = "select b.code,b.score from tdx.profit b where (b.`code`,b.statDate) in (select `code`,max(statDate) from tdx.profit group by `code`);"
    cur_profit.execute(profit_sql)
    profit_scores = cur_profit.fetchall()
    for item in profit_scores:
        code = item[0][3::]
        score = item[1]
        pdate = datetime.today().date()
        update_sql = f"UPDATE `tdx`.`score` SET profit='{score}',`date`='{pdate}' where `code`='{code}'; "
        cur_score.execute(update_sql)
        cnx.commit()

    # 更新turn和PER得分
    cur_turn_PER = cnx.cursor()
    turn_PER_sql = "select b.`code`,b.turn_score,b.PER_score from tdx.KPI b " \
                   "where (b.`code`,b.`date`) in (select `code`,max(`date`) from tdx.KPI group by `code`);"
    cur_turn_PER.execute(profit_sql)
    turn_PER_scores = cur_turn_PER.fetchall()
    for item in turn_PER_scores:
        code = item[0]
        turn = item[1]
        PER = item[2]
        tdate = datetime.today().date()
        update_sql = f"UPDATE `tdx`.`score` SET turn='{turn}',PER='{PER}',`date`='{tdate}' where `code`='{code}'; "
        cur_score.execute(update_sql)
        cnx.commit()

    cur_score.close()
    cur_balance.close()
    cur_growth.close()
    cur_profit.close()
    cur_turn_PER.close()
    cnx.close()


def dojob():
    scheduler = BlockingScheduler(max_instance=20)
    scheduler.add_job(update_balance, 'cron', hour=16, minute=8)
    scheduler.add_job(update_growth, 'cron', hour=18, minute=8)
    scheduler.add_job(update_profit, 'cron', hour=20, minute=8)
    scheduler.add_job(update_score, 'cron', hour=22, minute=8)
    scheduler.start()


dojob()
