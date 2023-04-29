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


df = ef.stock.get_quote_history('600519')

print(df)