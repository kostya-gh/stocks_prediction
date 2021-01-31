#!/usr/bin/env python3
import pymysql
import datetime
import pandas as pd
from finam import Exporter, Market, Timeframe


class DataHandler():
    """Get market data from finam.ru"""

    def __init__(self, code, market=Market.SHARES, timeframe=Timeframe.MINUTES1,
                 start_date=datetime.date(2010, 1, 1), end_date=None):
        self.code = code
        self.market = market
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date

    def download_data(self):
        """Import historical data from finam.ru"""
        exporter = Exporter()
        idx = exporter.lookup(code=self.code, market=self.market).index[0]
        data = exporter.download(
            id_=idx, market=self.market, timeframe=self.timeframe,
            start_date=self.start_date, end_date=self.end_date)
        data['<DATE>'] = data['<DATE>'].astype(str) + 'T' + data['<TIME>']
        data.drop('<TIME>', axis=1, inplace=True)
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        data['date'] = pd.to_datetime(data['date'], format='%Y%m%dT%H:%M:%S')
        return data

    def db_connection():
        """Connect to Stocks database"""
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='12345678',
                                     database='Stocks')
        return connection

    def save_data(self):
        """Download and save data into database"""
        connection = self.db_connection()
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.code};")
            cursor.execute(f"""
                            CREATE TABLE {self.code} (
                            timestamp DATETIME PRIMARY KEY,
                            open DECIMAL(7, 2) NOT NULL,
                            high DECIMAL(7, 2) NOT NULL,
                            low DECIMAL(7, 2) NOT NULL,
                            close DECIMAL(7, 2) NOT NULL,
                            volume BIGINT NOT NULL
                            );
                            """
                           )
            for row in self.download_data().values:
                cursor.execute(f"""
                                INSERT INTO {self.code} (timestamp, open, high,
                                                         low, close, volume)
                                VALUES ({repr(str(row[0]))}, {row[1]}, {row[2]},
                                        {row[3]}, {row[4]}, {row[5]});
                                """
                               )
            connection.commit()

    def read_data(self):
        """Read data from database"""
        connection = self.db_connection()
        with connection.cursor() as cursor:
