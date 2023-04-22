#-*- coding: UTF-8 -*-
# 从9:15开始记录竞价信息

import time
import json
import pymysql
import re
import baostock as bs
import efinance as ef
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


def update_auction():

    df = ef.stock.get_realtime_quotes()
    # 取出数据库当天的股票代码
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

        limit_time = '09:30:00'
        now_time = datetime.now().strftime("%H:%M:%S")

        cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],
                              database=info["database"])
        while now_time < limit_time:
            print(f"auction_efinance开始时间:{datetime.now()}")
            cur_today_index = cnx.cursor()
            index_today_sql = "select code from tdx.auction where left(auction_date,10)='%s';"
            cur_today_index.execute(index_today_sql % today)
            exist_codes = cur_today_index.fetchall()  # 已有的股票代码
            for item in exist_codes:
                exist_code_list.append(item[0])

            tvalue = []
            cur_insert_auction = cnx.cursor()

            for row in df.itertuples():
                if row.股票代码 not in exist_code_list and row.涨跌幅 != '-' and row.最新价 != '-':
                    if float(row.涨跌幅) > 9:
                        tvalue.append((row.股票代码, str(datetime.now())[0:19], float(row.最新价), float(row.涨跌幅)))
            if len(tvalue) != 0:
                insert_auction_sql = re.sub("\[|\]","",f"insert into auction(code,auction_date,auction_price,auction_gain) values{[ x for x in tvalue]}") + ";"
                # 写入数据库
                cur_insert_auction.execute(insert_auction_sql)
                cnx.commit()
            cur_insert_auction.close()
            time.sleep(60)
            print(f"auction_efinance结束时间:{datetime.now()}")

        cnx.close()
    bs.logout()


def dojob():
    scheduler = BlockingScheduler(timezone="Asia/Shanghai", max_instance=20)
    scheduler.add_job(update_auction, 'cron', hour=9, minute=15)
    scheduler.start()


dojob()
