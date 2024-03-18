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
    index_sql = "select code from tdx.index where concept like '%BK0500%';"
    cur_indexs.execute(index_sql)
    tdx_indexs = cur_indexs.fetchall()

    df = pd.DataFrame(tdx_indexs, columns=['code'])
    code_list = [combine(x[0]) for x in df.values]

    nums = 300  # 一次读取的数据
    headers = {'Referer': 'https://finance.sina.com.cn'}
    limit_time = '15:00:00'
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
            atime = datetime.datetime.now()
            for item in _text:
                _string = item
                _pattern = re.compile(r'var hq_str_\w+(\d{6})="(.+)";')
                _result = _pattern.match(_string)
                if _result is not None:
                    code = _result[1]
                    _info = _result[2]
                    _datas = re.split(r'\.*,', _info)

                    preclose_price = float(_datas[2])
                    total_volume = float(_datas[8])

                    buy1_volume = float(_datas[10])
                    buy1_price = float(_datas[11])
                    buy2_volume = float(_datas[12])
                    buy2_price = float(_datas[13])
                    buy3_volume = float(_datas[14])
                    buy3_price = float(_datas[15])
                    buy4_volume = float(_datas[16])
                    buy4_price = float(_datas[17])
                    buy5_volume = float(_datas[18])
                    buy5_price = float(_datas[19])

                    sell1_volume = float(_datas[20])
                    sell1_price = float(_datas[21])
                    sell2_volume = float(_datas[22])
                    sell2_price = float(_datas[23])
                    sell3_volume = float(_datas[24])
                    sell3_price = float(_datas[25])
                    sell4_volume = float(_datas[26])
                    sell4_price = float(_datas[27])
                    sell5_volume = float(_datas[28])
                    sell5_price = float(_datas[29])

                    today = _datas[30]
                    cur_time = _datas[31]

                    cur_auction_insert = (f"insert into bidding(code,date,total_volume,bvolume1,bprice1,bvolume2,bprice2,bvolume3,bprice3,bvolume4,bprice4,bvolume5,bprice5,"
                                          f"svolume1,sprice1,svolume2,sprice2,svolume3,sprice3,svolume4,sprice4,svolume5,sprice5) "
                                          f" values('{code}','{atime}',{total_volume},{buy1_volume},{buy1_price},{buy2_volume},{buy2_price},{buy3_volume},{buy3_price},{buy4_volume},{buy4_price},{buy5_volume},{buy5_price},"
                                          f"{sell1_volume},{sell1_price},{sell2_volume},{sell2_price},{sell3_volume},{sell3_price},{sell4_volume},{sell4_price},{sell5_volume},{sell5_price})")
                    cur_auction.execute(cur_auction_insert)
                    cnx.commit()

        before_time = datetime.datetime.now().strftime("%H:%M:%S")
        print(datetime.datetime.now())

    cur_auction.close()
    cur_indexs.close()
    cnx.close()
    print(datetime.datetime.now())


def dojob():
    scheduler = BlockingScheduler()
    scheduler.add_job(update_auction, 'cron', hour=9, minute=30)
    scheduler.start()


# dojob()
before_time = '12:50:00'
while before_time < '13:00:00':
    before_time = datetime.datetime.now().strftime("%H:%M:%S")
    continue

update_auction()
