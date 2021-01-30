#!/usr/bin/env python3
import pymysql
import pandas as pd
from finam import Exporter, Market, Timeframe


def load_data(code, market=Market.SHARES, timeframe=Timeframe.DAILY):
    """Import historical data from finam.ru"""
    exporter = Exporter()
    idx = exporter.lookup(code=code, market=market).index[0]
    data = exporter.download(id_=idx, market=market, timeframe=timeframe)
    data['<DATE>'] = data['<DATE>'].astype(str) + 'T' + data['<TIME>']
    data.drop('<TIME>', axis=1, inplace=True)
    data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    data['date'] = pd.to_datetime(data['date'], format='%Y%m%dT%H:%M:%S')
    return data


def save_data(code, market=Market.SHARES, timeframe=Timeframe.MINUTES1):
    """Download and save data into database"""
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='12345678',
                                 database='Stocks')
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {code};")
        cursor.execute(f"""
                        CREATE TABLE {code} (
                        date DATETIME PRIMARY KEY,
                        open DOUBLE NOT NULL,
                        high DOUBLE NOT NULL,
                        low DOUBLE NOT NULL,
                        close DOUBLE NOT NULL,
                        volume DOUBLE NOT NULL
                        );"""
                       )
        for row in load_data(code, market, timeframe).values:
            cursor.execute(f"""
                            INSERT INTO {code} 
                            VALUES ({row[0]}, {row[1]}, {row[2]}, 
                                    {row[3]}, {row[4]}, {row[5]});
                            """)


save_data('GAZP', timeframe=Timeframe.MONTHLY)
