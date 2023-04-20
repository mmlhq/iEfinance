import pymysql
import json


def update_board_score():

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_index = cnx.cursor()
    cur_index_sql = "select code,score from tdx.v_board_score;"
    cur_index.execute(cur_index_sql)
    items = cur_index.fetchall()

    for item in items:
        code = item[0]
        score = item[1]
        cur_update = cnx.cursor()
        update_sql = f"update tdx.score set board = '{score}' where code='{code}';"
        cur_update.execute(update_sql)
        cnx.commit()

    cur_index.close()
    cur_update.close()
    cnx.close()


update_board_score()
