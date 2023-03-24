# -*- coding: UTF-8 -*-
# 更新股票基本信息

import baostock as bs
import re
import pymysql
import json
import efinance as ef
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

# 更新或者添加股票代码、名称
def update_index():
    df = ef.stock.get_realtime_quotes()
    today = datetime.today().date()

    lg = bs.login()
    rs = bs.query_trade_dates(start_date=today)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    isTradeday = data_list[0][1]

    exist_code_list = []
    if isTradeday == '1':  # 如果是交易日则执行
        with open("config/config.json", encoding="utf-8") as f:
            cfg = json.load(f)
        info = cfg["mysql"]
        cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],
                              database=info["database"])
        cur_tdx_index = cnx.cursor()
        tdx_index_sql = "select right(code,6),name from tdx.index;"
        cur_tdx_index.execute(tdx_index_sql)
        tdx_indexs = cur_tdx_index.fetchall()  # 返回tuple的tuple
        update_name_sql = "update tdx.index set name='%s' where code='%s';"
        insert_code_name_sql = "insert into tdx.index(code,name) values('%s','%s');"
        cur_update_name = cnx.cursor()
        cur_insert_name = cnx.cursor()

        for row in df.itertuples():
            if tuple([row.股票代码, row.股票名称]) not in tdx_indexs:  # 如果不在数据库中
                # 如果查到代码，只是名称不同，则更新名称
                if row.股票代码 in [x[0] for x in tdx_indexs]:
                    cur_update_name.execute(update_name_sql % (row.股票名称, row.股票代码))
                # 两个都没有，则插入
                else:
                    cur_insert_name.execute(insert_code_name_sql % (row.股票代码, row.股票名称))
            cnx.commit()

        cur_tdx_index.close()
        cur_update_name.close()
        cur_insert_name.close()
        cnx.close()

    lg = bs.logout()


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

    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

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


def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_index, 'cron', hour=15, minute=8)
    scheduler.add_job(update_stock_basic, 'cron', hour=0, minute=32)
    scheduler.start()


dojob()
