#!/usr/bin/env python3
import pymysql
import pandas as pd
from datetime import date
from finam import Exporter, Market, Timeframe


class Data():
    """Get historical data from finam.ru"""

    def __init__(self, code, market=Market.SHARES, timeframe=Timeframe.MINUTES1,
                 start_date=date(2008, 1, 1), end_date=None):
        self.code = code
        self.market = market
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date

    def download(self):
        """Download historical data from finam.ru"""
        exporter = Exporter()
        idx = exporter.lookup(code=self.code, market=self.market).index[0]
        data = exporter.download(
            id_=idx, market=self.market, timeframe=self.timeframe,
            start_date=self.start_date, end_date=self.end_date)
        data['<DATE>'] = data['<DATE>'].astype(str) + 'T' + data['<TIME>']
        del data['<TIME>']
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        data['date'] = pd.to_datetime(data['date'], format='%Y%m%dT%H:%M:%S')
        return data.set_index('date')

    def save(self):
        """Download and save data into database"""
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='12345678',
                                     database='Stocks')
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS {self.code};')
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
            for row in self.download().values:
                cursor.execute(f"""
                                INSERT INTO {self.code} (timestamp, open, high,
                                                         low, close, volume)
                                VALUES ({repr(str(row[0]))}, {row[1]}, {row[2]},
                                        {row[3]}, {row[4]}, {row[5]});
                                """
                               )
            connection.commit()
        connection.close()


class MarketCap():
    """Get data about market capitalization"""

    def __init__(self):
        self.moex = 'https://www.moex.com/'
        self.init = 'a20'  # 4 квартал 2011 г.
        self.urls = (('a685', 'a1127', 'a1244', 'a1465'),
                     ('a1688', 'a1823', 'a2045', 'a2261'),
                     ('a2474', 'a2702', 'a2813', 'a2987'),
                     ('a3048', 'a3177', 'a3369', 'a3503'),
                     ('a3601', 'a3691', 'a3794', 'a3882'),
                     ('a4027', 'a4094', 'a4184', 'a4258'),
                     ('a4318', 'a4377', 'a4420', 'a6401'),
                     ('a6861', 'a6997', 'a7080', 'a7150'),
                     ('a7246', 'a7305', 'a7396', 'a7605'))  # квартальные за 2012-2020 гг.
        self.years = range(2012, 2021)
        self.quarters = range(1, 5)

    def write(self):
        """Write data to csv"""

        def parser(url):
            """Parse moex.com for data"""
            df = pd.read_html(url, index_col=0, thousands=' ', decimal=',')[0]
            df.columns = ['name', 'category', 'id', 'volume', 'price', 'cap']
            df.index.name = 'code'
            return df

        def string_handler(string):
            """Remove unnecesary charachters"""
            if type(string) == str:
                x = bytes(string, 'latin1')
                x = x.replace(b'\xa0', b'')
                x = x.replace(b',', b'.')
                x = x.decode('latin1')
                x = x.replace(' ', '', 10)
                return x
            return string

        df = parser(self.moex + self.init)

        for i, year in enumerate(self.years):
            for j, quarter in enumerate(self.quarters):
                df = df.merge(parser(self.moex + self.urls[i][j]), how='outer',
                              left_index=True, right_index=True,
                              suffixes=(None, '_' + str(year) + '_' + str(quarter)))
        for col in df.columns:
            if 'cap' in col:
                df[col] = df[col].apply(string_handler).astype(float)
            elif 'volume' in col:
                df[col] = pd.to_numeric(df[col].apply(
                    string_handler), errors='coerce')
        true_names = dict.fromkeys(df.index.values)
        true_categories = dict.fromkeys(df.index.values)
        true_ids = dict.fromkeys(df.index.values)
        for col in df.columns:
            if 'name' in col:
                for idx in true_names.keys():
                    if type(df[col].loc[idx]) == str:
                        true_names[idx] = df[col].loc[idx]
            elif 'category' in col:
                for idx in true_categories.keys():
                    if type(df[col].loc[idx]) == str:
                        true_categories[idx] = df[col].loc[idx]
            elif 'id' in col:
                for idx in true_ids.keys():
                    if type(df[col].loc[idx]) == str:
                        true_ids[idx] = df[col].loc[idx]
        df['name'] = df.merge(pd.DataFrame(pd.Series(true_names), columns=['true_names']),
                              left_index=True, right_index=True, how='inner')['true_names']
        df['category'] = df.merge(pd.DataFrame(pd.Series(true_categories), columns=['true_categories']),
                                  left_index=True, right_index=True, how='inner')['true_categories']
        df['id'] = df.merge(pd.DataFrame(pd.Series(true_ids), columns=['true_id']),
                            left_index=True, right_index=True, how='inner')['true_id']
        for col in df.columns:
            if ('name' in col or 'category' in col or 'id' in col)\
                    and (col != 'name' and col != 'category' and col != 'id'):
                del df[col]
        df.to_csv('market_cap.csv', sep=';', decimal=',')
