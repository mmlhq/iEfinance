# 扫描auction表中，涨
import pymysql
import json
import efinance as ef
import pandas as pd

with open("config/config.json", encoding="utf-8") as f:
    cfg = json.load(f)
info = cfg["mysql"]
cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"],database=info["database"])

# cur_item：auction中的每一项
cur_item = cnx.cursor()
aitem_sql = f"select code,close_gain from tdx.auction where close_gain is null;"
cur_item.execute(aitem_sql)
auction_item = cur_item.fetchall()

# 获取当天的开盘价、开盘涨幅、收盘价、收盘涨幅，更新tdx.auction表
stock_codes = []
for aitem in auction_item:
    stock_codes.append(aitem[0])

pd = ef.stock.get_latest_quote(stock_codes)

codes = pd["代码"]
print(type(codes))

data = pd[pd["代码"]=="002616"].head()

open_price = data["今开"].values[0]
yesterday_price = data["昨日收盘"].values[0]
open_gain = 0
if yesterday_price is not None:
    open_gain = open_price/yesterday_price * 100
close_price = data["最新价"].values[0]
close_gain = data["涨跌幅"].values[0]
high_price = data["最高"].values[0]
# 更新tdx
update_sql = f"update tdx.auction set open_price={open_price},open_gain={open_gain},close_price={close_price},close_gain={close_gain}"

if yesterday_price is not None:
    high_gain = high_price/yesterday_price * 100
if high_gain >= 9.5:
    next_price = high_price
    next_gain = high_gain
#    next_date = today
#    days = next_date - auction_date
    # 更新tdx




cur_item.close()
cnx.close()