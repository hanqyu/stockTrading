#-*- coding: utf-8 -*-

import scraping.scraping_from_naver as sc
import scraping.codes
from db.table import Table
import sys
import datetime
import math


def progressBar(name, value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '=' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("\rPercent: [{0}] {1}% - {2} Done".format(arrow + spaces, float(round(percent * 100, 1)), name))
    sys.stdout.flush()


# TODO-1 나중에 같은 날짜 && 같은 코드의 중복데이터가 쌓이면 제거해주는 작업 필요


start_time = datetime.datetime.now()
print("Scraping 시작시간: {0}\r".format(start_time))

codes = scraping.codes.codes()

table = Table()

for i, name, code in codes.itertuples(name=None):
    last_date = table.get_last_date(code)
    page_num = math.ceil((datetime.datetime.today() - last_date).days / 10)
    data = sc.getData(code, page_num=page_num)
    data = sc.preprocess(data, code, name)

    if last_date is not None:  # last_date가 없으면 통째로 다 넣게 됨
        data = data.drop(data[data['date'].apply(lambda x: x <= last_date)].index)
    data.to_sql(name=table.name, con=table.con, if_exists='append', index=False)
    table.commit()
    progressBar(name, codes[codes['corp_code'] == code].index[0], len(codes), bar_length=50)

print("모든 Scraping 완료!")
table.close()

end_time = datetime.datetime.now()
print("종료시간: {0}".format(end_time))
