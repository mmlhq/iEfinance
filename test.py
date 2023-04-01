# -*- coding: UTF-8 -*-
import efinance as ef
df_concept =  ef.stock.get_belong_board('000001')
concept = ""
for row in df_concept.itertuples():
    concept = concept +' ' + row.板块代码
print(concept.lstrip())
