#-*- coding: utf-8 -*-

import scraping.scraping_from_naver as sc
import scraping.codes
from db.table import Table
import sys
import datetime
from multiprocessing import Pool


def progressBar(name, value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("{0} Done\rPercent: [{1}] {2}%".format(name, arrow + spaces, int(round(percent * 100))))
    sys.stdout.flush()


def scrapeData(code):
    # code, name = corp['corp_code'], corp['corp_name']
    # code, name = tuple[1], tuple[2]
    data = sc.getData(code)
    # data = sc.preprocess(data, code, name)
    return data, code

start_time = datetime.datetime.now()
print("시작시간: {0}\r".format(start_time))
codes = scraping.codes.codes()

# pool = Pool(processes=4)
table = Table()

# for data in pool.map(scrapeData, codes.itertuples(name=None)):
# for data, code in pool.map(scrapeData, codes['corp_code'].tolist()):
for i, name, code in codes.itertuples(name=None):
    # name = codes[codes['corp_code'] == code].corp_name
    data = sc.getData(code)
    data = sc.preprocess(data, code, name)

    last_date = table.get_last_date(code)
    if last_date is not None:
        data = data.drop(data['date'].apply(lambda x: x <= last_date).index)
    data.to_sql(name=table.name, con=table.con, if_exists='append', index=False)
    table.commit()
    # print('table commit')
    progressBar(name, codes[codes['corp_code'] == code].index[0], len(codes), bar_length=50)

print("모든 Scraping 완료!")
table.close()


end_time = datetime.datetime.now()
print("종료시간: {0}".format(end_time))

# TODO-1 나중에 같은 날짜 && 같은 코드의 중복데이터가 쌓이면 제거해주는 작업 필요