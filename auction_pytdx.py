#-*- coding: UTF-8 -*-
import time
from datetime import datetime
import pymysql
import baostock as bs
import json
from apscheduler.schedulers.blocking import BlockingScheduler
from pytdx.hq import TdxHq_API

def market(code):
    match code[:1]:
        case '0' | '3':
            return 0
        case '6':
            return 1
        case '4' | '8':
            return 2
        case _:
            return


def update_auction():
    today = datetime.today().date()

    lg = bs.login()
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)
    rs = bs.query_trade_dates(start_date=today)
    print('query_trade_dates respond error_code:' + rs.error_code)
    print('query_trade_dates respond  error_msg:' + rs.error_msg)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    isTradeday = data_list[0][1]
    if isTradeday == '1':  # 如果是交易日则执行
        with open("config/config.json", encoding="utf-8") as f:
            cfg = json.load(f)
        info = cfg["mysql"]
        cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
        cur_index = cnx.cursor()
        index_sql = "select code from tdx.index where type='1' and outDate is null;"
        cur_index.execute(index_sql)
        database_index = cur_index.fetchall()
        index_lists = [(market(x[0]),x[0]) for x in database_index]

        api = TdxHq_API()
        with api.connect('119.147.212.81', 7709):
            cur_auction = cnx.cursor()
            limit_time = '09:30:00'
            now = datetime.now().strftime("%H:%M:%S")
            while now < limit_time:
                print(f"开始时间:{now}")
                for i in range(0, len(index_lists), 80):
                    data = api.get_security_quotes(index_lists[i:i+80])  # 一次最多只能返回80个股票数据
                    if data is not None:
                        for item in data:
                            if (item['price'] is not None) and (item['last_close'] is not None):
                                gain = (item['price']-item['last_close'])/item['last_close']*100
                                if gain > 9:
                                    cur_auction_insert = f"replace into auction(code,auction_date,auction_price,auction_gain) values('{item['code']}','{datetime.now()}','{item['price']}','{gain}')"
                                    cur_auction.execute(cur_auction_insert)
                                    cnx.commit()
                time.sleep(60)
                now = datetime.now().strftime("%H:%M:%S")
        cur_index.close()
        cnx.close()

def scan_data():
    today = datetime.today().date()

    lg = bs.login()
    rs = bs.query_trade_dates(start_date=today)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    isTradeday = data_list[0][1]
    if isTradeday == '1':  # 如果是交易日则执行
        with open("config/config.json", encoding="utf-8") as f:
            cfg = json.load(f)
        info = cfg["mysql"]
        cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
        cur_auction_index = cnx.cursor()
        auction_index_sql = "SELECT code,max(auction_date) FROM tdx.auction where next_date is null group by code;"

        cur_auction_index.execute(auction_index_sql)
        auction_index = cur_auction_index.fetchall() #返回的是元组格式 （code,datetime)

        index_lists = []
        for item in auction_index:
            market = item[0].split('.')[0]
            code = item[0].split('.')[1]
            if market == 'sz':
                index_lists.append((0,code))
            elif market == 'sh':
                index_lists.append((1,code))
            else:
                continue

        api = TdxHq_API()

        with api.connect('119.147.212.81', 7709):
            cur_auction = cnx.cursor()
            cur_auction_insert = "insert into auction(code,auction_date,auction_price,auction_gain) values('%s','%s','%f','%f')"

            limit_time = '09:30:00'
            now = datetime.now().strftime("%H:%M:%S")
            while now < limit_time:
                print(now)
                for i in range(0, len(index_lists), 80):
                    data = api.get_security_quotes(index_lists[i:i+80]) # 一次最多只能返回80个股票数据
                    if data is not None:
                        for item in data:
                            if (item['price'] is not None) and (item['last_close'] is not None):
                                gain = (item['price']-item['last_close'])/item['last_close']*100
                                if gain > 9:
                                    cur_auction.execute(cur_auction_insert % (item['code'], datetime.now(), item['price'], gain))
                                    cnx.commit()
                time.sleep(90)
                now = datetime.now().strftime("%H:%M:%S")
        cur_auction_index.close()
        cnx.close()

def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_auction, 'cron', hour=9, minute=15)
    scheduler.start()


dojob()
