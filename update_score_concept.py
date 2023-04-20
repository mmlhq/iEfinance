import pymysql
import json

def update_concept_score():
    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_concept_score = cnx.cursor()
    cur_index_concept = cnx.cursor()
    index_concept_sql = f"select code,concept from tdx.index;"
    cur_index_concept.execute(index_concept_sql)
    items = cur_index_concept.fetchall()
    for item in items:
        code = item[0]
        concepts = item[1].split(' ')
        score = 0
        for key in concepts:
            sql_concept_score = f"select score from tdx.concept where `key`='{key}';"
            cur_concept_score.execute(sql_concept_score)
            _score = cur_concept_score.fetchone()
            if _score is not None:
                if _score[0] > score:
                    score = _score[0]
        cur_update = cnx.cursor()
        update_sql = f"update tdx.score set concept = '{score}' where code='{code}';"
        cur_update.execute(update_sql)
        cnx.commit()

    cur_concept_score.close()
    cur_index_concept.close()
    cnx.close()


update_concept_score()
