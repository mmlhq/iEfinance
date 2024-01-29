# -*- coding: UTF-8 -*-
import datetime
from time import sleep
import pymysql
import requests
import json
import re
import pandas as pd
import baostock as bs
from apscheduler.schedulers.blocking import BlockingScheduler

def combine(code):
    match code[:1]:
        case '0' | '3':
            return 'sz' + code
        case '6':
            return 'sh' + code
        case '4' | '8':
            return 'bj' + code
        case _:
            return

def update_auction():
    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_indexs = cnx.cursor()
    index_sql = "select code from tdx.index;"
    cur_indexs.execute(index_sql)
    tdx_indexs = cur_indexs.fetchall()

    df = pd.DataFrame(tdx_indexs, columns=['code'])
    code_list = [combine(x[0]) for x in df.values]

    nums = 800  # 一次读取的数据
    headers = {'Referer': 'https://finance.sina.com.cn'}
    limit_time = '23:30:00'
    before_time = datetime.datetime.now().strftime("%H:%M:%S")
    cur_auction = cnx.cursor()
    print(datetime.datetime.now())

    today = datetime.date.today()
    lg = bs.login()
    rs = bs.query_trade_dates(start_date=today)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    isTradeday = data_list[0][1]

    # if isTradeday != "1":
    #     return

    while before_time < limit_time:
        for i in range(0, len(code_list), nums):
            codes = re.sub(r"\[|\]|\'", "", str(code_list[i:i + nums])).replace(" ", "")
            url = 'https://hq.sinajs.cn/list='+codes
            file = requests.get(url=url, headers=headers)
            # 完成返回数据的提取、判断和写入tdx操作
            _text = re.findall(r'.*?\n', file.text)
            for item in _text:
                _string = item
                _pattern = re.compile(r'var hq_str_\w+(\d{6})="(.+)";')
                _result = _pattern.match(_string)
                if _result is not None:
                    code = _result[1]
                    _info = _result[2]
                    _datas = re.split(r'\.*,', _info)
                    preclose_price = float(_datas[2])
                    buy1_price = float(_datas[6])
                    atime = datetime.datetime.now()
                    if preclose_price is not None and preclose_price != 0:
                        a_gains = (buy1_price - preclose_price) / preclose_price * 100
                        if a_gains >= 8:
                            cur_auction_insert = f"replace into auction(code,auction_date,auction_price,auction_gain) values('{code}','{atime}','{buy1_price}','{a_gains}')"
                            cur_auction.execute(cur_auction_insert)
                            cnx.commit()

        before_time = datetime.datetime.now().strftime("%H:%M:%S")
        print(datetime.datetime.now())
        sleep(60)

    cur_auction.close()
    cur_indexs.close()
    cnx.close()
    print(datetime.datetime.now())


def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_auction, 'cron', hour=9, minute=15)
    scheduler.start()


# dojob()
update_auction()
