import sqlite3
from typing import Optional
import sys, getopt, os
import time 
import json
import math
import requests
from collections import Counter
import numpy as np


class DatabaseHandler:

    def __init__(self, abs_path: str):
        """ Creates or uses existing database located in {abs_path} and stores its connection """
        try:
            self.conn = sqlite3.connect(str(abs_path))
            assert self.conn is not None
        except sqlite3.Error as e:
            # database could not be created (or opened) -> abort
            print(e)
            sys.exit(1)
        self.exchange_stack =  ['SS_lm_bs', 'SS_lm_sb', 'SS_mm_bs', 'SS_mm_sb',  
                                'SF_lm_bs', 'SF_lm_sb', 'SF_mm_bs', 'SF_mm_sb', 
                                'FS_lm_bs', 'FS_lm_sb', 'FS_mm_bs', 'FS_mm_sb',
                                'FF_lm_bs', 'FF_lm_sb', 'FF_mm_bs', 'FF_mm_sb']
        # Gets tickers
        self.binance_futures_tickers = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        self.binance_futures_tickers = self.binance_futures_tickers.json()
        self.binance_futures_tickers = [item['symbol'] for item in self.binance_futures_tickers['symbols']]
        # Bitget futures
        self.bitget_futures_stream = "wss://ws.bitget.com/mix/v1/stream"
        self.bitget_futures_tickers = [e['symbol'].split('_')[0] for e in json.loads(requests.get("https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl").text)['data']]
        # Binance spot
        self.binance_spot_tickers = requests.get("https://api.binance.com/api/v3/exchangeInfo")
        self.binance_spot_tickers = self.binance_spot_tickers.json()
        self.binance_spot_tickers = [item['symbol'] for item in self.binance_spot_tickers['symbols']]
        # Bitget spot
        self.bitget_spot_stream = "wss://ws.bitget.com/spot/v1/stream"
        self.bitget_spot_tickers = [e['symbol'].split('_')[0] for e in json.loads(requests.get("https://api.bitget.com/api/spot/v1/public/products").text)['data']]
        # Dictionary of columns of each table
        self.symbols_dictionary = {}
        for exchange in self.exchange_stack:
            self.symbols_dictionary[exchange] = []
            if exchange in ['SS_lm_bs', 'SS_lm_sb', 'SS_mm_bs', 'SS_mm_sb']:
                cnt = Counter(self.binance_spot_tickers + self.bitget_spot_tickers)
                tickers = [k for k, v in cnt.items() if v > 1 and 'USDT' in k]
                for ticker in tickers:
                    self.symbols_dictionary[exchange].append(ticker)
            if exchange in ['SF_lm_bs', 'SF_lm_sb', 'SF_mm_bs', 'SF_mm_sb']:
                cnt = Counter(self.binance_futures_tickers + self.bitget_spot_tickers)
                tickers = [k for k, v in cnt.items() if v > 1]
                for ticker in tickers:
                    self.symbols_dictionary[exchange].append(ticker)
            if exchange in ['FS_lm_bs', 'FS_lm_sb', 'FS_mm_bs', 'FS_mm_sb']:
                cnt = Counter(self.binance_spot_tickers + self.bitget_futures_tickers)
                tickers = [k for k, v in cnt.items() if v > 1]
                for ticker in tickers:
                    self.symbols_dictionary[exchange].append(ticker)
            if exchange in ['FF_lm_bs', 'FF_lm_sb', 'FF_mm_bs', 'FF_mm_sb']:
                cnt = Counter(self.binance_futures_tickers + self.bitget_futures_tickers)
                tickers = [k for k, v in cnt.items() if v > 1]
                for ticker in tickers:
                    self.symbols_dictionary[exchange].append(ticker)

    def create_table_prices(self) -> None:
        try:
            for table in self.exchange_stack:
                self.conn.cursor().execute(f"""CREATE TABLE {table}(timestamp STRING PRIMARY KEY)""")
                self.conn.commit()
        except sqlite3.Error as e:
            print('creation failed:')
            print(e)

    def remove_table(self, tablename: str) -> None:
        sql = "DROP TABLE {}".format(tablename)
        try:
            self.conn.cursor().executescript(sql)
            self.conn.commit()
        except sqlite3.Error as e:
            print('removing failed:')
            print(e)

    def insert_table(self, table, ticker): 
            try:
                sql = 'ALTER TABLE {} ADD {} FLOAT;'.format(table, ticker.replace('1', 'one').replace('1000', 'thousand')) 
                self.conn.cursor().execute(sql)
                self.conn.commit()
            except sqlite3.Error as e:
                print('Insertion failed')
                print(e, ticker)

    def insert_columns(self):
            

        for exchange in self.symbols_dictionary.keys():
            for ticker in self.symbols_dictionary[exchange]:
                self.insert_table(exchange, ticker)

# .replace('1', 'one').replace('1000', 'thousand')
    def insert_many(self, data):

        for stack in data.keys():

            try:
                #col = [x.split('___')[0].split('_')[-1] for x in list(data.keys())]
                col = [x.replace('1', 'one').replace('1000', 'thousand') for x in list(data[stack].keys())]
                #print(col)
                column_names = ','.join(col) 
                inputs_list = '?,' * (len(list(data[stack].keys())))
                inputs_list =  inputs_list[:-1].strip()
                self.conn.cursor().execute(
                        f"""INSERT INTO {stack}({column_names}) VALUES({inputs_list});""", tuple(list(data[stack].values()))
                        )
                self.conn.commit()
            except sqlite3.Error as e:
                print('insertion failed:')
                print(e)

    # Spread handlers

    def estimated_profit_calculator(self, data, amount):
  
        binance_futures_market_fee = 0.03
        binance_futures_limit_fee = 0.012
        bitget_futures_market_fee = 0.06
        bitget_futures_limit_fee = 0.02
        binance_spot_market_fee = 0.1
        binance_spot_limit_fee = 0.1
        bitget_spot_market_fee = 0.1
        bitget_spot_limit_fee = 0.1


        data = dict(list(data.items()))
        exchange_stack =  self.exchange_stack
        arb = {}
        for x in exchange_stack:
            arb[x] = {}
            arb[x]['timestamp'] = data['timestamp']
        for key in [x for x in data.keys() if 'binance_futures' in x]:
            for key_2 in [x for x in data.keys() if 'bitget_futures' in x]:
                    if key_2.split('_')[-1] == key.split('_')[-1]:
                            try:    
                                    # Limit long spot, market short futures
                                    a = self.slippage_calculator(depth=data[key], amount=amount, side='sell') 
                                    b = self.slippage_calculator(depth=data[key_2], amount=amount, side='sell') # On practice, make the ask the closest to the price
                                    result = self.get_change_percentage(b, a) 
                                    ticker = key_2.split('_')[-1]
                                    arb['FF_lm_bs'][ticker] = result

                                    # Market long spot, market short futures
                                    a = self.slippage_calculator(depth=data[key], amount=amount, side='sell')
                                    b = self.slippage_calculator(depth=data[key_2], amount=amount, side='buy')
                                    result = self.get_change_percentage(b, a) 
                                    arb['FF_mm_bs'][ticker] = result

                                    # Limit short spot, market long futures
                                    a = self.slippage_calculator(depth=data[key], amount=amount, side='buy')
                                    b = self.slippage_calculator(depth=data[key_2], amount=amount, side='buy')
                                    result = self.get_change_percentage(a, b) 
                                    arb['FF_lm_sb'][ticker] = result
                                    
                                    # Limit short spot, market long futures
                                    a = self.slippage_calculator(depth=data[key], amount=amount, side='buy')
                                    b = self.slippage_calculator(depth=data[key_2], amount=amount, side='sell')
                                    result = self.get_change_percentage(a, b) 
                                    arb['FF_mm_sb'][ticker] = result
                            except:
                                    pass
        return arb


    def slippage_calculator(self, depth, amount, side):
        """
            Returns medium of the order after slippage
        """
        if side == 'sell': 
            filled_prices, filled_amounts = [], []
            acc = 0
            for bid in depth['bids']:
                price = float(bid[0])
                amount_filled = float(bid[1])
                acc += amount_filled
                if acc < amount:
                    filled_amounts.append(amount_filled)
                    filled_prices.append(price)
                if acc > amount:
                    filled_prices.append(price)
                    filled_amounts.append(amount+amount_filled-acc)
                    break
            try:
                return np.average(filled_prices, weights= filled_amounts)
            except:
                return None

        if side == 'buy': 
          filled_prices, filled_amounts = [], []
          acc = 0
          for ask in depth['asks']:
              price = float(ask[0])
              amount_filled = float(ask[1])
              acc += amount_filled
              if acc < amount:
                  filled_amounts.append(amount_filled)
                  filled_prices.append(price)
              if acc > amount:
                  filled_prices.append(price)
                  filled_amounts.append(amount+amount_filled-acc)
                  break
          try:
              return np.average(filled_prices, weights= filled_amounts)
          except:
              return None

    def get_change_percentage(self, current, previous):
        if current == previous:
            return 0
        else:
            return ((current - previous) / previous) * 100.0


