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


# def update_close():
#     df = ef.stock.get_realtime_quotes()
#     # 取出数据库当天的股票代码
#     today = datetime.today().date()
#
#     lg = bs.login()
#     rs = bs.query_trade_dates(start_date=today)
#
#     data_list = []
#     while (rs.error_code == '0') & rs.next():
#         data_list.append(rs.get_row_data())
#     isTradeday = data_list[0][1]
#
#     exist_code_list = []
#     if isTradeday == '1':  # 如果是交易日则执行
#         with open("config/config.json", encoding="utf-8") as f:
#             cfg = json.load(f)
#         info = cfg["mysql"]
#
#         cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],
#                               database=info["database"])
#         cur_tdx_auction = cnx.cursor()
#         auction_sql = f"select * from tdx.auction where left(auction_date,10)='{today}' and close_price is null;"
#         cur_tdx_auction.fetchall()
#
#         for row in df.itertuples():
#
#
#         cur_update_close = cnx.cursor()
#         sql
#
#         cnx.close()
#     bs.logout()
#
#
# def dojob():
#     scheduler = BlockingScheduler(timezone="Asia/Shanghai", max_instance=20)
#     scheduler.add_job(update_auction, 'cron', hour=9, minute=15)
#     scheduler.start()
#
#
# update_auction()
