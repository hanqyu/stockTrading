#-*- coding: utf-8 -*-

import scraping.codes
from db.table import Table
from analytics.rsi import RSI
from multiprocessing import Pool
import sys
import datetime
import pandas as pd


def progressBar(name, value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '=' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("\rPercent: [{0}] {1}% - {2} Done".format(arrow + spaces, float(round(percent * 100, 1)), name))
    sys.stdout.flush()


def get_row_for_rsi(code):
    values2 = ['date', 'code','name', 'close']
    df2 = pd.DataFrame(table.get_rows(code, values2), columns=values2)
    return df2


# TODO-1 나중에 같은 날짜 && 같은 코드의 중복데이터가 쌓이면 제거해주는 작업 필요

start_time = datetime.datetime.now()
print("RSI 계산 시작시간: {0}".format(start_time))
codes = scraping.codes.codes()
result = pd.DataFrame(columns=['date', 'code', 'name', 'rsi'])

table = Table()
values = ['date', 'code', 'name', 'close']

with Pool(processes=4) as pool:
    for df in pool.map(get_row_for_rsi, codes['corp_code'].tolist()):
        RSI(df)
        code = df.iloc[-1]['code']
        name = df.iloc[-1]['name']
        # if int(datetime.datetime.now().strftime("%H")) <= 16:
        #     bool = True
        # else:
        #     bool = datetime.datetime.today().strftime('%Y-%m-%d') == table.get_last_date(code).strftime('%Y-%m-%d')  # 오늘일경우

        rsi = df[df['date'] == table.get_last_date(code).strftime('%Y-%m-%d %H:%M:%S')].iloc[-1]
        if rsi['rsi'] <= 30):
            result = result.append(rsi, ignore_index=True)

        progressBar(name, codes[codes['corp_code'] == code].index[0], len(codes), bar_length=50)

print(result)
file_name = "RSI_{0}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %H시%M분"))
result.to_csv(file_name)
print("{0} 저장 완료".format(file_name))
end_time = datetime.datetime.now()
print("종료시간: {0}".format(end_time))
print("{0}분 소요".format(int((end_time-start_time).seconds/60)))
