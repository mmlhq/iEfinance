import baostock as bs
import pandas as pd
import json
import pymysql

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

def put_kdata():
#### 登陆系统 ####
    lg = bs.login()

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_index = cnx.cursor()
    cur_index_sql = "select code from tdx.index;"
    cur_index.execute(cur_index_sql)
    tdx_indexs = cur_index.fetchall()
    cur_kata = cnx.cursor()

    for tindex in tdx_indexs:
        code = combine(tindex[0])
        #### 获取沪深A股历史K线数据 ####
        # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
        # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
        # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
        rs = bs.query_history_k_data_plus(code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
            start_date='2024-03-22', end_date='2024-03-22',
            frequency="d", adjustflag="3")

            #### 打印结果集 ####

        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            # row_str = ','.join(str(i) for i in rs.get_row_data())
            row = tuple([-99999 if i == '' else i for i in rs.get_row_data()])

            print(row[1])
            sql_insert_kata = f"insert into kdata(date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTm,isST) values{row}"
            cur_kata.execute(sql_insert_kata)
    cnx.commit()

    cur_kata.close()
    cur_index.close()
    cnx.close()
    #### 登出系统 ####
    bs.logout()

put_kdata()