# -*- coding: UTF-8 -*-
# 更新股票基本信息

import baostock as bs
import re
import pymysql
import json
import efinance as ef
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

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
                    insert_code_name_sql = f"insert into tdx.index(code,name,tradeStatus,ipoDate,type,status,board,concept,ROE,PER) values('{row.股票代码}','{row.股票名称}','1','{ipoDate}','1','1','{board}','{concept}');"
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


def get_base_info():  # 通过efinance模块更新换手率、ROE（净资产收率率）、PER（市盈率）
    df = ef.stock.get_realtime_quotes() # 定时在15:00时后运行

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_insert = cnx.cursor()
    cur_score = cnx.cursor()
    for row in df.itertuples():
        code = row.股票代码
        date = datetime.today().date()
        turn =  row.换手率
        PER = row.动态市盈率
        # insert_sql = f"insert into KPI(code,date,turn,PER) values('{code}','{date}','{turn}','{PER}')"
        # cur_insert.execute(insert_sql)
        turn_score = caculate_score('turn',turn)
        PER_score = caculate_score('PER', PER)
        index_in_score = f"select code from tdx.score where code='{code}';"
        cur_score.execute(index_in_score)
        xcode = cur_score.fetchone()
        if xcode is None:
            insert_score_sql=f"insert score(code,date,turn,PER) values('{code}','{date}','{turn_score}','{PER_score}');"
            cur_score.execute(insert_score_sql)
        else:
            update_score_sql=f"update score set turn='{turn_score}',PER='{PER_score}';"


        cnx.commit()
    cur_insert.close()
    cnx.close()

def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_index, 'cron', hour=9, minute=40)
    scheduler.add_job(update_stock_basic, 'cron', hour=22, minute=00)
    scheduler.add_job(update_trade_status, 'cron', hour=12, minute=00)
    scheduler.start()


# dojob()
# update_index()
get_base_info()
