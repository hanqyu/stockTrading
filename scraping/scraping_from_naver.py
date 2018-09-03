import requests
import pandas as pd


'''
출처: http://excelsior-cjh.tistory.com/109 [EXCELSIOR]
'''


def preprocess(df, code, name):
    # 한글로 된 컬럼명을 영어로 바꿔줌
    df = df.rename(columns= {'날짜': 'date', '종가': 'close', '전일비': 'diff',
                             '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})

    #  데이터의 타입을 int형으로 바꿔줌
    df[['close', 'diff', 'open', 'high', 'low', 'volume']] \
        = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)

    # 컬럼명 'date'의 타입을 date로 바꿔줌
    df['date'] = pd.to_datetime(df['date'])

    # 일자(date)를 기준으로 오름차순 정렬
    df = df.sort_values(by=['date'], ascending=True)

    # 기업code와 기업name 열 추가
    df['code'] = pd.Series(code, index=df.index)
    df['name'] = pd.Series(name, index=df.index)
    return df


def getData(code, page_num=20):
    url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
    df = pd.DataFrame()
    for page in range(1, page_num+1):
        pg_url = '{url}&page={page}'.format(url=url, page=page)
        df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

    return preprocess(df.dropna())