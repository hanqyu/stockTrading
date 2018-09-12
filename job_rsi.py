#-*- coding: utf-8 -*-

import scraping.codes
from db.table import Table, TableSecondary
from analytics.rsi import rsi
from multiprocessing import Pool
from sys import stdout
import pandas as pd
import datetime


def progressBar(name, value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '=' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    stdout.write("\rPercent: [{0}] {1}% - {2} Done".format(arrow + spaces, float(round(percent * 100, 1)), name))
    stdout.flush()


def get_row_for_rsi(code):
    values = ['date', 'code', 'name', 'close', 'volume']
    df = pd.DataFrame(table_origin.get_rows(code, values), columns=values)
    df['volume-price'] = df.apply(lambda x: x['close'] * x['volume'])
    return df, code


start_time = datetime.datetime.now()
print("RSI 계산 시작시간: {0}".format(start_time))
codes = scraping.codes.codes()
result = pd.DataFrame(columns=['date', 'code', 'name', 'rsi'])

table_origin = Table()
table_secondary = TableSecondary("daily_rsi")

with Pool(processes=4) as pool: # job_rsi가 main이 아니면 pool에서 오류남
    for df, code in pool.map(get_row_for_rsi, codes['corp_code'].tolist()):
        df = rsi(df)
        table_last_date = table_secondary.get_last_date(code)

        # 테이블에 저장
        if table_last_date is not None:  # table_last_date가 없으면 통째로 다 넣게 됨
            df = df.drop(df[df['date'].apply(lambda x: x <= table_last_date)].index)
        df[['date', 'code', 'name', 'rsi']].to_sql(
            name=table_secondary.name, con=table_secondary.con, if_exists='append', index=False)

        # result에 저장
        if df.iloc[-1]['rsi'] <= 30):
            result = result.append(df.iloc[-1], ignore_index=True)

        # progress bar
        progressBar(df.iloc[-1]['name'], codes[codes['corp_code'] == code].index[0], len(codes), bar_length=50)

end_time = datetime.datetime.now()
file_name = "RSI_{0}.csv".format(end_time.strftime("%Y-%m-%d %H시%M분"))
result.sort_values(by=['volume-price', 'rsi'], ascending=False).to_csv(file_name)
print(result.tail())
print("{0} 저장 완료".format(file_name))
print("종료시간: {0}".format(end_time))
print("{0}분 소요".format(int((end_time-start_time).seconds/60)))
