#-*- coding: UTF-8 -*-
# 获取实时行情的所有股票信息
# 更新或写入tdx.index

import efinance as ef

pd = ef.stock.get_realtime_quotes()

print(pd.columns)