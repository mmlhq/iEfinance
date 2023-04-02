# -*- coding: UTF-8 -*-
# 更新股票基本信息

import baostock as bs
import re
import pandas as pd
import numpy as np
import pymysql
import json
import efinance as ef
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

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

# 更新或者添加股票代码、名称
def update_index():
    df = ef.stock.get_realtime_quotes()

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],database=info["database"])
    cur_tdx_index = cnx.cursor()
    tdx_index_sql = "select code,name from tdx.index;"
    cur_tdx_index.execute(tdx_index_sql)
    tdx_indexs = cur_tdx_index.fetchall()  # 返回tuple的tuple

    cur_update_name = cnx.cursor()
    cur_insert_name = cnx.cursor()

    for row in df.itertuples():
        if tuple([row.股票代码, row.股票名称]) not in tdx_indexs:  # 如果不在数据库中
            # 如果查到代码，只是名称不同，则更新名称
            if row.股票代码 in [x[0] for x in tdx_indexs]:
                update_name_sql = f"update tdx.index set name='{row.股票名称}' where code='{row.股票代码}';"
                cur_update_name.execute(update_name_sql)
            # 两个都没有，则插入股票代码、名称、所属板块、概念
            else:
                df_concept = ef.stock.get_belong_board(row.股票代码)
                concept = ""
                for row in df_concept.itertuples():
                    concept = concept + ' ' + row.板块代码
                concept = concept.lstrip()
                df_info = ef.stock.get_base_info(row.股票代码)
                PER = df_info['市盈率(动)']
                ROE = df_info['市净率']
                board = df_info['板块编号']
                insert_code_name_sql = f"insert into tdx.index(code,name,tradeStatus,type,status,board,concept) values('{row.股票代码}','{row.股票名称}','1','1','1','{board}','{concept}');"
                if row.股票名称[0:1] == 'N':
                    ipoDate = str(datetime.today().date())
                    insert_code_name_sql = f"insert into tdx.index(code,name,tradeStatus,ipoDate,type,status,board,concept) values('{row.股票代码}','{row.股票名称}','1','{ipoDate}','1','1','{board}','{concept}');"
                cur_insert_name.execute(insert_code_name_sql)
        cnx.commit()

    cur_tdx_index.close()
    cur_update_name.close()
    cur_insert_name.close()
    cnx.close()

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


def update_stock_basic():
    lg = bs.login()

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_index = cnx.cursor()
    index_stock_basic_sql = "select code,name,ipoDate,outDate,type,status from tdx.index;"
    cur_index.execute(index_stock_basic_sql)
    database_index_stock_basic = cur_index.fetchall()
    database_index_codes = [x[0] for x in database_index_stock_basic]

    stock_basic_info = ['code', 'name', 'ipoDate', 'outDate', 'type', 'status']
    update_sql = "UPDATE  `index` SET `name`=%s,`ipoDate`=%s,`outDate`=%s,`type`=%s,`status`=%s where `code`=%s; "

    for code in database_index_codes:
        rs = bs.query_stock_basic(combine(code))
        data_list = rs.get_row_data()
        if data_list != [] :
            if data_list[2] == '':
                data_list[2] = None
            if data_list[3] == '':
                data_list[3] = None
            if tuple(data_list) not in database_index_stock_basic:
                rs_item = (data_list[1], data_list[2], data_list[3], data_list[4], data_list[5], code)
                cur_index.execute(update_sql, rs_item)
                cnx.commit()

    cur_index.close()
    cnx.close()
    bs.logout()


def update_trade_status():
    lg = bs.login()

    today = datetime.today().date()
    rs = bs.query_trade_dates(start_date=today)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    isTradeday = data_list[0][1]

    if isTradeday == '1':
        # 连接本地数据库
        with open("config/config.json", encoding="utf-8") as f:
            cfg = json.load(f)
        info = cfg["mysql"]
        cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
        cur_index = cnx.cursor()
        index_trade_status_sql = "select code,tradeStatus from tdx.index;"
        cur_index.execute(index_trade_status_sql)
        tdx_indexs = cur_index.fetchall()  # 元组项的元组（（code,tradeStatus），...
        update_sql = "update tdx.index set tradeStatus='%s' where code='%s'"
        rs = bs.query_all_stock()  # 查询交易状态,缺省是当前时间
        while (rs.error_code == '0') & rs.next():
            rs_item = rs.get_row_data()
            rs_item_code = rs_item[0]
            rs_item_status = rs_item[1]
            if rs_item_code[:6] == 'sz.399' or rs_item_code[:6] == 'sh.000':
                continue
            if (rs_item_code[3:], rs_item_status) not in tdx_indexs:
                cur_index.execute(update_sql % (rs_item_status, rs_item_code[3:]))
                cnx.commit()
        cur_index.close()
        cnx.close()

    lg = bs.logout()


def get_base_info():  # 通过efinance模块更新KPI表的换手率、PER（市盈率）
    np = (ef.stock.get_realtime_quotes()).to_numpy() # 定时在15:00时后运行

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_score = cnx.cursor()
    score_indexs_sql = f"select code from tdx.score;"
    cur_score.execute(score_indexs_sql)
    score_indexs = cur_score.fetchall()
    print(datetime.now())
    replace_values = []
    pd_level = leveltable_to_df(cnx)
    print(pd_level)
    for row in  np:  # df.itertuples():
        code = row[0]
        date = str(datetime.today().date())
        turn =  row[8]
        PER = row[10]
        turn_score = caculate_score('turn',turn, pd_level)
        if PER == '-':
            PER_score = 0
        else:
            PER_score = caculate_score('PER', PER, pd_level)
        replace_values.append(tuple([code,date,turn_score,PER_score]))

    if replace_values:
        s = re.sub(r"\[", "", str(replace_values))
        s =  re.sub(r"\)]", ");", s)
        replace_values_str = f"replace into score(code,date,turn,PER) values{s}"
        cur_score.execute(replace_values_str)
        cnx.commit()

    print(datetime.now())
    cur_score.close()
    cnx.close()


def dojob():
    scheduler = BlockingScheduler(max_instance=20)
    scheduler.add_job(update_index, 'cron', hour=21, minute=20)
    scheduler.add_job(get_base_info, 'cron', hour=23, minute=30)
    scheduler.add_job(update_stock_basic, 'cron', hour=0, minute=30)
    scheduler.add_job(update_trade_status, 'cron', hour=1, minute=00)
    scheduler.start()


dojob()
