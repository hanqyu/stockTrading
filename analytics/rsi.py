import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from db.table import Table
import datetime
from scraping.codes import codes


def rsi(df, period=14):
    delta = df['close'].diff().dropna()
    u = delta * 0
    d = u.copy()
    u[delta > 0] = delta[delta > 0]
    d[delta < 0] = -delta[delta < 0]
    u[u.index[period-1]] = np.mean( u[:period] ) #first value is sum of avg gains
    u = u.drop(u.index[:(period-1)])
    d[d.index[period-1]] = np.mean( d[:period] ) #first value is sum of avg losses
    d = d.drop(d.index[:(period-1)])
    rs = u.ewm(com=period-1, adjust=False).mean() / d.ewm(com=period-1, adjust=False).mean()
    rsi = 100 - 100 / (1 + rs)
    df['rsi'] = rsi
    return df


# TODO 벤치마크 측정해서 최적화방향고려하자. 매번 모든 컬럼할수가없음 + RSI 저장해야함