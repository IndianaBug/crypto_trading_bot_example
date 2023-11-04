import json 
import time
import requests
import logging

from binance.spot import Spot as Binance_Spot 
from binance.um_futures import UMFutures as Binance_UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import math
import bitget_v1.spot.wallet_api as spot_wallet_bitget
import bitget_v1.mix.position_api as futures_position_bitget
import bitget_v1.spot.account_api as spot_accounts_bitget
import bitget_v1.mix.account_api as futures_accounts_bitget
import bitget_v1.spot.order_api as spot_order_bitget
import bitget_v1.mix.order_api as futures_order_bitget
import bitget_v1.spot.market_api as spot_market_bitget
import bitget_v1.mix.market_api as futures_market_bitget
import bitget_v1.spot.public_api as spot_public_bitget
from bitget_v2.mix.order_api import OrderApi as futures_OrderApi_2
from bitget_v2.spot.wallet_api import WalletApi as spot_walletApi_2
import hmac
import base64
import json
import time
from decimal import Decimal
import math
import datetime
import csv
import os
import random
import numpy as np


class Executor():

    def __init__(self, apis, 
                 amount_percentage_to_trade, 
                 open_spread_percentage, 
                 open_close_spread_difference, 
                 stop_percentage, 
                 transfer_difference,
                 spread_expansion, 
                 is_testing=False):
        """
        """
        self.amount_percentage_to_trade = amount_percentage_to_trade
        self.open_spread_percentage = open_spread_percentage
        self.open_close_spread_difference = open_close_spread_difference
        self.stop_percentage = stop_percentage
        self.transfer_difference=transfer_difference
        self.spread_expansion=spread_expansion
        # Exchange APIS
        self.binance_master_api = apis['binance_master_api']
        self.binance_master_secret = apis['binance_master_secret']
        self.binance_subaccount_api = apis['binance_subaccount_api']
        self.binance_subaccount_secret = apis['binance_subaccount_secret']
        self.bitget_master_api = apis['exchange_master_api']
        self.bitget_master_secret = apis['exchange_master_secret']
        self.bitet_master_password = apis['bitget_master_password']
        self.bitget_subaccount_api = apis['exchange_subaccount_api']
        self.bitget_subaccount_secret = apis['exchange_subaccount_secret']
        self.bitet_subaccount_password = apis['bitget_subaccount_password']
        # Binace master and subaccount
        self.binance_client_master_spot = Binance_Spot(self.binance_master_api, self.binance_master_secret, show_header=True, base_url="https://api-gcp.binance.com")                     
        self.binance_client_subaccount_spot = Binance_Spot(self.binance_subaccount_api, self.binance_subaccount_secret, show_header=True, base_url="https://api-gcp.binance.com")
        self.binance_client_master_futures = Binance_UMFutures(key=self.binance_master_api, secret=self.binance_master_secret)#, base_url="https://api.binance.com")
        self.binance_client_subaccount_futures = Binance_UMFutures(key=self.binance_subaccount_api, secret=self.binance_subaccount_secret)#, base_url="https://api.binance.com")
        # Bitget master
        self.bitget_master_walletApi_spot = spot_wallet_bitget.WalletApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_accountApi_spot = spot_accounts_bitget.AccountApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_orderApi_spot = spot_order_bitget.OrderApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_marketApi_spot = spot_market_bitget.MarketApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_positionApi_futures = futures_position_bitget.PositionApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_accountApi_futures = futures_accounts_bitget.AccountApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_orderApi_futures = futures_order_bitget.OrderApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_marketApi_futures = futures_market_bitget.MarketApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_master_publicAPi_spot = spot_public_bitget.PublicApi(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_v2_master_orderApi = futures_OrderApi_2(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        self.bitget_v2_master_walletApi = spot_walletApi_2(self.bitget_master_api, self.bitget_master_secret, self.bitet_master_password, use_server_time=False, first=False)
        # Bitget subaccount
        self.bitget_subaccount_walletApi_spot = spot_wallet_bitget.WalletApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_accountApi_spot = spot_accounts_bitget.AccountApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_orderApi_spot = spot_order_bitget.OrderApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_marketApi_spot = spot_market_bitget.MarketApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_positionApi_futures = futures_position_bitget.PositionApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_accountApi_futures = futures_accounts_bitget.AccountApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_orderApi_futures = futures_order_bitget.OrderApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_subaccount_marketApi_futures = futures_market_bitget.MarketApi(self.bitget_subaccount_api, self.bitget_subaccount_secret, self.bitet_subaccount_password, use_server_time=False, first=False)
        self.bitget_master_ID = apis['bitget_master_ID']
        self.bitget_subaccount_ID = apis['bitget_subaccount_ID']
        self.bitget_master_withdrawal_pass = apis['bitget_master_withdrawal_pass']
        ######################################################################################################
        # Minimum stepSize for position
        # Binance spot
        self.binance_spot_stepSize = {}
        response = self.binance_client_master_spot.exchange_info()
        for index, symbol in enumerate(response['data']['symbols']):
            self.binance_spot_stepSize[symbol['symbol']] = response['data']['symbols'][index]['filters'][1]['stepSize']
        for key in self.binance_spot_stepSize.keys():
            dec = Decimal(str(self.binance_spot_stepSize[key]))
            dec = abs(dec.as_tuple().exponent)
            self.binance_spot_stepSize[key] = str(dec)

        # Binance futures
        self.binance_futures_stepSize = {}
        response = self.binance_client_master_futures.exchange_info()
        for index, symbol in enumerate(response['symbols']):
            self.binance_futures_stepSize[symbol['symbol']] = response['symbols'][index]['filters'][1]['stepSize']
        for key in self.binance_futures_stepSize.keys():
            dec = Decimal(str(self.binance_futures_stepSize[key]))
            dec = abs(dec.as_tuple().exponent)
            self.binance_futures_stepSize[key] = str(dec)
        

        # Bitget spot
        self.bitget_spot_stepsize = {}
        response = json.loads(requests.get('https://api.bitget.com/api/spot/v1/public/products').text)
        for e in response['data']:
            self.bitget_spot_stepsize[e['symbolName']] = e['quantityScale'] # If it is 4, it mean you may place an order of 1.1111
        # Bitget futures
        self.bitget_futures_stepsize = {}
        response = json.loads(requests.get('https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl').text)
        for e in response['data']:
            self.bitget_futures_stepsize[e['symbolName']] = e['sizeMultiplier']
        ############################################################################################################
        # Modes
        self.mode_check_balances = True
        self.mode_offset_balances= False
        self.mode_trade = False
        self.mode_checkRecord = False
        # Balances
        self.free_usdt_binance = 0
        self.free_usdt_bitget = 0
        self.position_usdt_amount = 0
        self.trading_instrument = ""
        self.position_base_amount_binance = 0
        self.position_base_amount_bitget = 0
        # Fees
        self.binance_spot_fee = 0.001
        self.bitget_spot_fee = 0.001
        self.binance_futures_fee = 0.0003
        self.bitget_futures_fee = 0.0006
        # USDT spot adresses
        self.binance_usdt_adress = self.binance_client_master_spot.deposit_address(coin="USDT", chain='ARBITRUM')['data']['address'] 
        self.bitget_usdt_adress = self.bitget_master_walletApi_spot.depositAddress(coin='USDT', chain='arbitrumone')['data']['address']
        # Transfer_step
        self.transfer_exchange = ''
        self.offset_balances_stage_1 = True
        self.offset_balances_stage_2 = False
        self.amount_to_withdraw = 0
        # Dealing with positions
        self.strategy = ''
        self.opening_spread = ''
        self.trading_instrument = ''
        ##
        # Open_position helping variables
        ##
        # Order Id's
        self.binance_open_limit_Id = ''
        self.binance_open_limit_client_Id = ''
        self.binance_open_stop_Id = ''
        self.binance_open_stop_client_Id = ''
        self.bitget_open_limit_Id = ''
        self.bitget_open_stop_Id = ''
        self.bitget_open_limit_Oid=''
        self.bitget_open_stop_Oid = ''
        # Order price and quantities
        self.binance_open_quantity = ''
        self.bitget_open_quantity = ''
        self.binance_open_price = ''
        self.bitget_open_price= ''
        self.binance_open_stop_price= ''
        self.bitget_open_stop_price=''
        # Stages of opening the position
        self.opening_stage_1 = True
        self.opening_stage_2 = False
        # Constrains to navigate stages
        self.open_helper_is_binance_limit_filled = False
        self.open_helper_is_bitget_limit_filled = False
        self.open_helper_is_biget_stop_triggered = False
        self.open_helper_is_binance_stop_triggered = False
        ##
        # Closen_position helping variables
        ##
        # Order Id's
        self.binance_close_limit_Id = ''
        self.binance_close_limit_client_Id = ''
        self.binance_close_stop_Id = ''
        self.binance_close_stop_client_Id = ''
        self.bitget_close_stop_Oid = ''
        self.bitget_close_stop_Id = ''
        self.bitget_close_limit_Id = ''
        self.bitget_close_limit_Oid = ''
        # Order price and quantities
        self.binance_close_quantity = self.binance_open_quantity
        self.bitget_close_quantity = self.bitget_open_quantity
        self.binance_close_price = ''
        self.bitget_close_price= ''
        self.binance_close_stop_price= ''
        self.bitget_close_stop_price=''
        # Stages of closing the position
        self.closing_stage_1 = False
        self.closing_stage_2 = False
        # Constrains to navigate stages
        self.close_helper_is_binance_limit_filled = False
        self.close_helper_is_bitget_limit_filled = False
        self.close_helper_is_biget_stop_triggered = False
        self.close_helper_is_binance_stop_triggered = False
        #
        self.is_testing = is_testing



    def main(self, spreads):

        start = time.time()

        if self.mode_check_balances == True:
            self.get_usdt_balances()
        if self.mode_offset_balances == True:
            self.offset_balances()
        if self.mode_trade == True:
            self.trader(spreads)
        if self.mode_checkRecord == True:
            self.write_record()

        print(f"Seconds taken to perform the action: {start - time.time()}")


    def get_usdt_balances(self):

        self.free_usdt_binance = float(self.get_balance(exchange='binance', type_='futures', account='subaccount', coin='USDT'))
        if self.is_testing == True:
            print("---------------------------------------------")
            print("Checkpiont_get_binance_balances")
            print("---------------------------------------------")
        self.free_usdt_bitget = float(self.get_balance(exchange='bitget', type_='futures', account='master', coin='USDT'))
        if self.is_testing == True:
            print("---------------------------------------------")
            print("Checkpiont_get_bitget_balances")
            print("---------------------------------------------")
        self.position_usdt_amount = min(float(self.free_usdt_binance), float(self.free_usdt_bitget)) * float(self.amount_percentage_to_trade)
                
        difference_binance = self.get_change_percentage(self.free_usdt_binance, self.free_usdt_bitget)
        difference_bitget = self.get_change_percentage(self.free_usdt_bitget, self.free_usdt_binance)

        print(difference_binance, difference_bitget)
        print(self.free_usdt_binance, self.free_usdt_bitget)


        if difference_binance > self.transfer_difference or difference_bitget > self.transfer_difference:
            self.mode_offset_balances = True
            self.mode_check_balances = False
            return
        else:
            self.mode_trade = True
            self.mode_check_balances = False
            return



    def offset_balances(self):
        
        """
            Bitget to binance works
            Binance to bitget works
        """
        
        balancesd_amount = (self.free_usdt_binance + self.free_usdt_bitget) / 2 * 0.99

        # Binacne case
        if self.free_usdt_binance > self.free_usdt_bitget and self.offset_balances_stage_1 == True:
            amount_to_withdraw = abs(balancesd_amount - self.free_usdt_binance) * 0.95
            amount_to_withdraw = round(amount_to_withdraw, 2)
            self.amount_to_withdraw = amount_to_withdraw
            self.transfer_exchange = 'binance'
            self.transfer_withdrawal(exchange='binance', from_='futures_subaccount', to_='spot_subaccount', coin='USDT', amount=self.amount_to_withdraw)
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont_offset balances binance futurese to spot subaccount transfer")
                print("---------------------------------------------")
            time.sleep(1.5)
            self.transfer_withdrawal(exchange='binance', from_='spot_subaccount', to_='spot_master', coin='USDT', amount=self.amount_to_withdraw)
            if self.is_testing == True:

                print("---------------------------------------------")
                print("Checkpiont_offset balances binance spot subaccount to spot master transfer")
                print("---------------------------------------------")
            time.sleep(1.5)
            self.transfer_withdrawal(exchange='binance', from_='spot_master', to_='spot_master_bitget', coin='USDT', amount=self.amount_to_withdraw)
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont_offset balances binance to bitget withdrawal")
                print("---------------------------------------------")
            self.offset_balances_stage_1 = False
            self.offset_balances_stage_2 = True
            time.sleep(30)
            return
        
        # Bitget case
        if self.free_usdt_binance < self.free_usdt_bitget and self.offset_balances_stage_1 ==  True:
            amount_to_withdraw = abs(balancesd_amount - self.free_usdt_bitget) * 0.95
            amount_to_withdraw = round(amount_to_withdraw, 2)
            self.amount_to_withdraw = amount_to_withdraw
            self.transfer_exchange = 'bitget'
            self.transfer_withdrawal(exchange='bitget', from_='futures_master', to_='spot_master', coin='USDT', amount=str(self.amount_to_withdraw))
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont_offset balances bitget futurese t spot transfer")
                print("---------------------------------------------")

            time.sleep(15)
            
            try:
                self.bitget_futures_withdrawal(
                    coin="USDT",
                    transferType="on_chain",
                    address=self.binance_usdt_adress,
                    chain="ARBITRUMONE",
                    size=str(self.amount_to_withdraw)
                )
            except Exception as e:
                print(e)

            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont_offset balances binance to bitget withdrawal")
                print("---------------------------------------------")
            self.offset_balances_stage_1 = False
            self.offset_balances_stage_2 = True
            time.sleep(30)
            return

        if self.offset_balances_stage_2 == True:

            if self.transfer_exchange == 'binance':
                print("---------------------------------------------")
                print("Checkpiont_offset balances get bitget balances")
                print("---------------------------------------------")
                amount = float(self.get_balance(exchange='bitget', type_='spot', account='master', coin='USDT'))
                time.sleep(5)
                if float(amount) >= self.amount_to_withdraw:
                    self.transfer_withdrawal(exchange='bitget', from_='spot_master', to_='futures_master', coin='usdt', amount=self.amount_to_withdraw)
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont_offset balances bitget spot to futures transfer (final step)")
                        print("---------------------------------------------")
                    self.offset_balances_stage_1 = True
                    self.offset_balances_stage_2 = False
                    self.mode_trade = True
                    self.mode_offset_balances = False
                    self.transfer_exchange = ''
                    return
                else:
                    return 

            if self.transfer_exchange == 'bitget':
                print("---------------------------------------------")
                print("Checkpiont_offset balances get binance balances")
                print("---------------------------------------------")
                amount = float(self.get_balance(exchange='binance', type_='spot', account='master', coin='USDT'))
                print("---------------------------------------------")
                print("Checkpiont_offset balances get binance balances")
                print("---------------------------------------------")
                print(amount)
                time.sleep(5)
                if float(amount) >= self.amount_to_withdraw:
                    self.transfer_withdrawal(exchange='binance', from_='spot_master', to_='spot_subaccount', coin='USDT', amount=float(self.amount_to_withdraw))
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont_offset balances binance spot_master to spot_subaccount transfer (final step)")
                        print("---------------------------------------------")
                    time.sleep(5)
                    self.transfer_withdrawal(exchange='binance', from_='spot_subaccount', to_='futures_subaccount', coin='USDT', amount=self.amount_to_withdraw)
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont_offset balances binance spot_subaccount to futures_subaccount transfer (final step)")
                        print("---------------------------------------------")
                    self.offset_balances_stage_1 = True
                    self.offset_balances_stage_2 = False
                    self.mode_trade = True
                    self.mode_offset_balances = False
                    self.transfer_exchange = ''
                    return
                else:
                    return 

    def count_decimals(self, number):
        try:
            decimal_count = str(number)[::-1].index('.')
        except ValueError:
            decimal_count = 0
        return int(decimal_count)
    
    def round_up_to_custom_decimal(self, number, decimal_places):
        factor = 10 ** decimal_places
        return math.ceil(number * factor) / factor

    def trader(self, spreads):
        """
            Check if the spreads ARE TURE maybe make some more API calls to get true PRICES
        """
        if self.opening_stage_1 == True and self.opening_stage_2 == False:
            # Select the highest spread and calculate the exact amount to open positions
            best_spreads = self.spreads_calculator_books(spreads)

            self.trading_instrument = [x for x in best_spreads.keys()][0]  
            self.strategy = best_spreads[self.trading_instrument]['strategie']
            self.opening_spread  = best_spreads[self.trading_instrument]['profit']
            print("THE BEST SPREAD: ", self.trading_instrument, self.strategy, self.opening_spread)
            if self.opening_spread > self.open_spread_percentage:
                # Calculate exact prices of limits and stops
                if  self.strategy == 'sell_buy':
                    self.binance_open_price = float(best_spreads[self.trading_instrument]['binance_ask']) 
                    self.bitget_open_price = float(best_spreads[self.trading_instrument]['bitget_bid']) 
                    print(self.binance_open_price, self.bitget_open_price)
                    self.binance_open_price = round(self.binance_open_price * (1 + self.spread_expansion), self.count_decimals(self.binance_open_price))
                    self.bitget_open_price = round(self.bitget_open_price  * (1 - self.spread_expansion), self.count_decimals(self.bitget_open_price))
                    self.bitget_side = 'Buy'
                    self.bitget_side_stop = 'Sell'
                    self.binance_side_limit = 'SELL'
                    self.binance_side_stop = 'BUY'
                    self.bitget_open_stop_price = self.round_up_to_custom_decimal(float(self.bitget_open_price* (1-self.stop_percentage)), self.count_decimals(self.bitget_open_price))
                    self.binance_open_stop_price = self.round_up_to_custom_decimal(float(self.binance_open_price* (1+self.stop_percentage)), self.count_decimals(self.binance_open_price))
                if self.strategy == 'buy_sell':
                    self.binance_open_price = float(best_spreads[self.trading_instrument]['binance_bid']) 
                    self.bitget_open_price = float(best_spreads[self.trading_instrument]['bitget_ask']) 
                    print(self.binance_open_price, self.bitget_open_price)
                    self.binance_open_price = round(self.binance_open_price * (1 - self.spread_expansion), self.count_decimals(self.binance_open_price))
                    self.bitget_open_price = round(self.bitget_open_price  * (1 + self.spread_expansion), self.count_decimals(self.bitget_open_price))
                    self.bitget_side = 'Sell'
                    self.bitget_side_stop = 'Buy'
                    self.binance_side_limit = 'BUY'
                    self.binance_side_stop = 'SELL'
                    self.bitget_open_stop_price = self.round_up_to_custom_decimal(float(self.bitget_open_price* (1+self.stop_percentage)), self.count_decimals(self.bitget_open_price))
                    self.binance_open_stop_price = self.round_up_to_custom_decimal(float(self.binance_open_price* (1-self.stop_percentage)), self.count_decimals(self.binance_open_price))
            # Binance
            self.binance_open_quantity = self.position_usdt_amount / self.binance_open_price
            binance_step_size = self.binance_futures_stepSize[self.trading_instrument]
            self.binance_open_quantity = math.floor(self.binance_open_quantity * 10**int(binance_step_size))/10**(int(binance_step_size))
            #Bitget
            self.bitget_open_quantity = self.position_usdt_amount / self.bitget_open_price
            bitget_step_size = self.bitget_futures_stepsize[self.trading_instrument]
            self.bitget_open_quantity = math.floor(self.bitget_open_quantity * 10**int(self.count_decimals(bitget_step_size)))/10**(int(self.count_decimals(bitget_step_size)))
            # Place bitget limit and set bitget stop

            
            try:
                
                self.bitget_open_limit_Oid = "".join(random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(64))

                data = self.bitget_futures_palceOrder(symbol=self.trading_instrument,
                                                                            productType='usdt-futures',
                                                                            marginMode='crossed',
                                                                            marginCoin='USDT',
                                                                            size=str(float(self.bitget_open_quantity)),
                                                                            price=str(self.bitget_open_price),
                                                                            side=self.bitget_side,
                                                                            tradeSide='',
                                                                            orderType='limit',
                                                                            force='',
                                                                            reduceOnly='',
                                                                            presetStopSurplusPrice='',
                                                                            presetStopLossPrice='',
                                                                            clientOid=self.bitget_open_limit_Oid)
                
                #print(data)
                self.bitget_open_limit_Id = data['data']['orderId']
                #print(self.bitget_open_limit_id)
                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiont place bitget order (opening)")
                    print("---------------------------------------------")
                #print(self.bitget_open_limit_id)
                #print(self.bitget_open_limit_Oid )

            except Exception as e:
                print(e)
                return
            try:

                self.bitget_open_stop_Oid = "".join(random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(50))

                data = self.bitget_futures_TriggerOrder(
                                    planType="normal_plan", 
                                    symbol=self.trading_instrument, 
                                    productType="usdt-futures",
                                    marginMode="crossed",
                                    marginCoin="USDT",
                                    size=str(self.bitget_open_quantity),
                                    price="",
                                    callbackRatio="",
                                    triggerPrice=str(self.bitget_open_stop_price),
                                    triggerType="mark_price",
                                    side=self.bitget_side_stop.lower(),
                                    tradeSide="",
                                    orderType="market",
                                    clientOid=self.bitget_open_stop_Oid,
                                    reduceOnly="YES",
                                    stopSurplusTriggerPrice="",
                                    stopSurplusExecutePrice="",
                                    stopSurplusTriggerType="",
                                    stopLossTriggerPrice="",
                                    stopLossExecutePrice="",
                                    stopLossTriggerType=""
                )

                self.bitget_open_stop_id = data['data']['orderId']


            except Exception as e:
                print(e)
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont place bitget order (opening)")
                print("---------------------------------------------")
            # Place binance limit
            try:

                # https://github.com/binance/binance-futures-connector-python/blob/main/binance/um_futures/account.py#L81
                binance_data = self.binance_client_subaccount_futures.new_order(symbol=self.trading_instrument,
                                                                                side=self.binance_side_limit,
                                                                                type="LIMIT",
                                                                                quantity=float(self.binance_open_quantity),
                                                                                timeInForce="GTC",
                                                                                price=float(str(self.binance_open_price)),
                                                                                positionSide='BOTH')
                self.binance_open_limit_Id = binance_data['orderId']
                self.binance_open_limit_client_Id = binance_data['clientOrderId']
                logging.info(self.binance_open_limit_Id)
                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiont place binance order (opening)")
                    print("---------------------------------------------")

            except ClientError as error:
                logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                print("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                return
            
            # Place binance stop
            try:
                binance_stop_check = self.binance_client_subaccount_futures.new_order(symbol=self.trading_instrument,
                                                                            side=self.binance_side_stop,
                                                                            type="STOP_MARKET",
                                                                            quantity=float(self.binance_open_quantity),
                                                                            timeInForce="GTC",
                                                                            stopPrice=self.binance_open_stop_price)
                self.binance_open_stop_Id = binance_stop_check['orderId']
                self.binance_open_stop_client_Id = binance_stop_check['clientOrderId']
                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiont place binance stop (opening)")
                    print("---------------------------------------------")
            except ClientError as error:
                logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                print("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                return
            self.opening_stage_2 = True
            self.opening_stage_1 = False
            return

        if self.opening_stage_2 == True and self.opening_stage_1 == False:
            
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont check bitget limit (opening)")
                print("---------------------------------------------")
                print(self.bitget_open_stop_Id, self.bitget_open_stop_Oid)

            # Check bitget
            try:
                bitget_order_check =  self.bitget_futures_orderDeitail(symbol=self.trading_instrument, 
                                                                       productType="USDT-FUTURES", 
                                                                       orderId=self.bitget_open_limit_Id,
                                                                       clientOid=self.bitget_open_limit_Oid)
                #bitget_order_check =  self.bitget_master_orderApi_futures.detail(symbol=self.trading_instrument + "_UMCBL", orderId=self.bitget_open_limit_id)
                print(bitget_order_check)
            except Exception as e:
                print(e)
                return
            
            #print(bitget_order_check['data']['state'])
            self.open_helper_is_bitget_limit_filled = True if bitget_order_check['data']['state'] == 'filled' else False

            
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont check bitget stop (opening)")
                print("---------------------------------------------")
                print(self.bitget_open_stop_Id, self.bitget_open_stop_Oid)

            try:
                bitget_stop_check =  self.bitget_futures_orders_plan_pending(
                                    orderId=self.bitget_open_stop_Id, 
                                    clientOid='', 
                                    symbol=self.trading_instrument,
                                    planType='normal_plan',
                                    productType='usdt-futures',
                                    idLessThan='',
                                    startTime='',
                                    endTime='',
                                    limit=''
                )
                #bitget_order_check =  self.bitget_master_orderApi_futures.detail(symbol=self.trading_instrument + "_UMCBL", orderId=self.bitget_open_limit_id)

            except Exception as e:
                print(e)
                return


            self.open_helper_is_biget_stop_triggered = False if bitget_stop_check['data']['entrustedList'][0]['planStatus'] == 'live' else True
            
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont check binance limit (opening)")
                print("---------------------------------------------")


            try:
                binance_order_ckeck_limit =  self.binance_client_subaccount_futures.query_order(
                                                                    symbol=self.trading_instrument,
                                                                    orderId=self.binance_open_limit_Id,
                                                                    origClientOrderId=self.binance_open_limit_client_Id )
                print(binance_order_ckeck_limit)
            except Exception as e:
                print(e)


            if binance_order_ckeck_limit['origQty'] == binance_order_ckeck_limit['executedQty']:
                self.open_helper_is_binance_limit_filled = True
            #print(binance_order_ckeck_limit['origQty'])
            # Check binance stop
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiont check binance stop (opening)")
                print("---------------------------------------------")
            try:

                binance_order_ckeck =  self.binance_client_subaccount_futures.query_order(
                                                                                symbol=self.trading_instrument,
                                                                                orderId=self.binance_open_stop_Id,
                                                                                origClientOrderId=self.binance_open_stop_client_Id )  
                print(binance_order_ckeck_limit)     
            except Exception as e:
                print(e)
            if binance_order_ckeck['origQty'] == binance_order_ckeck['executedQty']:
                self.open_helper_is_binance_stop_triggered = True
            #print(binance_order_ckeck['origQty'])


            print('bitget stop: ',   bitget_stop_check['data']['entrustedList'][0]['planStatus'],
                  'bitget limit',   bitget_order_check['data']['state'],
                  'bitget stop, ',  binance_order_ckeck['origQty'], binance_order_ckeck['executedQty'],
                  'binance limit', binance_order_ckeck_limit['origQty'], binance_order_ckeck_limit['executedQty']
                  )
            #time.sleep(50)

            # Constrains
            if  (self.open_helper_is_binance_limit_filled == True and 
                self.open_helper_is_bitget_limit_filled == True and 
                self.open_helper_is_biget_stop_triggered == False and
                self.open_helper_is_binance_stop_triggered == False):
                
                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiont limits filled cancel bitget stop (opening)")
                    print("---------------------------------------------")
                # Cancel bitget stop
                try:

                    bitget_data = self.bitget_futures_cancelTrigger(symbol=self.trading_instrument,
                                                            productType="usdt-futures",
                                                            orderIdList=[
                                                                {
                                                                    "orderId": self.bitget_open_stop_Id,
                                                                    "clientOid": self.bitget_open_stop_Oid
                                                                }
                                                            ],
                                                            marginCoin='USDT')

                except Exception as e:
                    print(e)
                    return
                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiont limits filled cancel binance stop (opening)")
                    print("---------------------------------------------")     
                # Cancel binance stop
                try:
                    self.binance_client_subaccount_futures.cancel_order(symbol=self.trading_instrument, 
                                                                        orderId=self.binance_open_stop_Id, 
                                                                        origClientOrderId=self.binance_open_stop_client_Id)
           
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                self.opening_stage_2 = False
                self.closing_stage_1 = True
                self.open_helper_is_binance_limit_filled = False  
                self.open_helper_is_bitget_limit_filled = False  
                self.open_helper_is_biget_stop_triggered = False 
                self.open_helper_is_binance_stop_triggered = False

                return
            if  (self.open_helper_is_binance_limit_filled == True and 
                self.open_helper_is_bitget_limit_filled == False and 
                self.open_helper_is_biget_stop_triggered == False and
                self.open_helper_is_binance_stop_triggered == True):
                
                print("---------------------------------------------")
                print("Checkpiont binance triggered filled cancel bitget limit (opening)")
                print("---------------------------------------------")   
                try:
                    bitget_data = self.bitget_futures_cancelOrder(symbol=self.trading_instrument,
                                                                    productType="usdt-futures",
                                                                    orderId=self.bitget_open_limit_Id,
                                                                    clientOid=self.bitget_open_limit_Oid)
                except Exception as e:
                    print(e)

                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiont binance triggered filled cancel bitget STOP (opening)")
                    print("---------------------------------------------")     
                try:
                    bitget_data = self.bitget_futures_cancelTrigger(symbol=self.trading_instrument,
                                                            productType="usdt-futures",
                                                            orderIdList=[
                                                                {
                                                                    "orderId": self.bitget_open_stop_Id,
                                                                    "clientOid": self.bitget_open_stop_Oid
                                                                }
                                                            ],
                                                            marginCoin='USDT')
                except Exception as e:
                    print(e)

                self.opening_stage_2 = False
                self.mode_checkRecord = True
                self.open_helper_is_binance_limit_filled = False  
                self.open_helper_is_bitget_limit_filled = False  
                self.open_helper_is_biget_stop_triggered = False 
                self.open_helper_is_binance_stop_triggered = False
                return
            if  (self.open_helper_is_binance_limit_filled == False and 
                self.open_helper_is_bitget_limit_filled == True and 
                self.open_helper_is_biget_stop_triggered == True and
                self.open_helper_is_binance_stop_triggered == False):
                try:
                    self.binance_client_subaccount_futures.cancel_order(symbol=self.trading_instrument, orderId=self.binance_open_stop_Id, origClientOrderId=self.binance_open_stop_client_Id)
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont bitget triggered filled cancel binance stop (opening)")
                        print("---------------------------------------------")                 
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                try:
                    self.binance_client_subaccount_futures.cancel_order(symbol=self.trading_instrument, orderId=self.binance_open_limit_Id, origClientOrderId=self.binance_open_limit_client_Id)
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont bitget triggered filled cancel binance limit (opening)")
                        print("---------------------------------------------")                   
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                self.opening_stage_2 = False
                self.mode_checkRecord = True
                self.open_helper_is_binance_limit_filled = False  
                self.open_helper_is_bitget_limit_filled = False  
                self.open_helper_is_biget_stop_triggered = False 
                self.open_helper_is_binance_stop_triggered = False
                return
        
        if self.closing_stage_1 == True:

            print('I am in the stage closing')
            # Check spreads
            profit = spreads[self.strategy][self.trading_instrument]['profit']
            if self.opening_spread - profit > self.open_close_spread_difference:
                if  self.strategy == 'sell_buy':
                    self.binance_close_price = float(spreads[self.strategy][self.trading_instrument]['binance_bid'])
                    self.bitget_close_price = float(spreads[self.strategy][self.trading_instrument]['bitget_ask'])
                    self.binance_close_price = round(self.binance_close_price * (1 + self.spread_expansion), self.count_decimals(self.binance_close_price))
                    self.bitget_close_price = round(self.bitget_close_price  * (1 - self.spread_expansion), self.count_decimals(self.bitget_close_price))
                    self.bitget_side = 'Sell'
                    self.bitget_side_stop = 'Sell'
                    self.binance_side_limit = 'BUY'
                    self.binance_side_stop = 'BUY'
                    self.bitget_close_stop_price = self.round_up_to_custom_decimal(float(self.bitget_close_price* (1 - self.stop_percentage)), self.count_decimals(self.bitget_close_price))
                    self.binance_close_stop_price = self.round_up_to_custom_decimal(float(self.binance_close_price* (1 + self.stop_percentage)), self.count_decimals(self.binance_close_price))
                    
                if self.strategy == 'buy_sell':
                    self.binance_close_price = float(spreads[self.strategy][self.trading_instrument]['binance_ask'])
                    self.bitget_close_price = float(spreads[self.strategy][self.trading_instrument]['bitget_bid'])
                    self.binance_close_price = round(self.binance_close_price * (1 - self.spread_expansion), self.count_decimals(self.binance_close_price))
                    self.bitget_close_price = round(self.bitget_close_price  * (1 + self.spread_expansion), self.count_decimals(self.bitget_close_price))
                    self.bitget_side = 'Buy'
                    self.bitget_side_stop = 'Buy'
                    self.binance_side_limit = 'SELL'
                    self.binance_side_stop = 'SELL'
                    self.bitget_close_stop_price = self.round_up_to_custom_decimal(float(self.bitget_close_price* (1 + self.stop_percentage)), self.count_decimals(self.bitget_close_price))
                    self.binance_close_stop_price = self.round_up_to_custom_decimal(float(self.binance_close_price* (1 - self.stop_percentage)), self.count_decimals(self.binance_close_price))
                self.binance_close_quantity =  self.binance_open_quantity
                self.bitget_close_quantity =  self.bitget_open_quantity
                # Modify bitget stop
                try:
                    self.bitget_close_stop_Oid = "".join(random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(50))

                    # bitget_data = self.bitget_futures_modifyOrder(orderId=self.bitget_open_limit_Id_new, 
                    #                                                         clientOid=self.bitget_open_limit_Oid_new, 
                    #                                                         symbol=self.trading_instrument, 
                    #                                                         newClientOid=self.bitget_close_stop_Oid, 
                    #                                                         newPresetStopLossPrice=self.bitget_close_stop_price,
                    #                                                         productType='USDT-FUTURES') 
                    bitget_data = self.bitget_futures_TriggerOrder(
                                        planType="normal_plan", 
                                        symbol=self.trading_instrument, 
                                        productType="usdt-futures",
                                        marginMode="crossed",
                                        marginCoin="USDT",
                                        size=str(self.bitget_open_quantity),
                                        price="",
                                        callbackRatio="",
                                        triggerPrice=str(self.bitget_close_stop_price),
                                        triggerType="mark_price",
                                        side=self.bitget_side_stop.lower(),
                                        tradeSide="",
                                        orderType="market",
                                        clientOid=self.bitget_close_stop_Oid,
                                        reduceOnly="YES",
                                        stopSurplusTriggerPrice="",
                                        stopSurplusExecutePrice="",
                                        stopSurplusTriggerType="",
                                        stopLossTriggerPrice="",
                                        stopLossExecutePrice="",
                                        stopLossTriggerType=""
                    )
                    self.bitget_close_stop_Id = bitget_data['data']['orderId']
                   
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont place bitget stop (closing)")
                        print("---------------------------------------------")     
                except Exception as e:
                    print(e)
                    return
                # Place bitget limit
                try:
                    self.bitget_close_limit_Oid = "".join(random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(50))
                    bitget_data = self.bitget_futures_palceOrder(symbol=self.trading_instrument,
                                                                    productType='usdt-futures',
                                                                    marginMode='crossed',
                                                                    marginCoin='USDT',
                                                                    size=str(float(self.bitget_open_quantity)),
                                                                    price=str(self.bitget_close_price),
                                                                    side=self.bitget_side,
                                                                    tradeSide='',
                                                                    orderType='limit',
                                                                    force='',
                                                                    reduceOnly='',
                                                                    presetStopSurplusPrice='',
                                                                    presetStopLossPrice='',
                                                                    clientOid=self.bitget_close_limit_Oid)
                    
                    self.bitget_close_limit_Id = bitget_data['data']['orderId']
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont place bitget limit (closing)")
                        print("---------------------------------------------")  
                except Exception as e:
                    print(e)
                    return
                # Place binance limit order
                try:
                    # https://github.com/binance/binance-futures-connector-python/blob/main/binance/um_futures/account.py#L81
                    binance_data = self.binance_client_subaccount_futures.new_order(symbol=self.trading_instrument,
                                                                                    side=self.binance_side_limit,
                                                                                    type="LIMIT",
                                                                                    quantity=float(self.binance_close_quantity ),
                                                                                    timeInForce="GTC",
                                                                                    price=float(str(self.binance_close_price )),
                                                                                    positionSide='BOTH' )
                    self.binance_close_limit_Id = binance_data['orderId']
                    self.binance_close_limit_client_Id = binance_data['clientOrderId']
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont place binance limit (closing)")
                        print("---------------------------------------------")  
                    logging.info(self.binance_close_limit_Id)
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                
                # Place binance stop
                try:
                    binance_stop_check = self.binance_client_subaccount_futures.new_order(symbol=self.trading_instrument,
                                                                                side=self.binance_side_stop,
                                                                                type="STOP_MARKET",
                                                                                quantity=float(self.binance_open_quantity),
                                                                                timeInForce="GTC",
                                                                                #reduceOnly='yes'
                                                                                stopPrice=self.binance_close_stop_price)
                    self.binance_close_stop_Id = binance_stop_check['orderId']
                    self.binance_close_stop_client_Id = binance_stop_check['clientOrderId']
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiont place binance stop (closing)")
                        print("---------------------------------------------")  

                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                self.closing_stage_2 = True
                self.closing_stage_1 = False
                return

        if self.closing_stage_2 == True:
            # Check bitget limit
            bitget_order_check =  self.bitget_futures_orderDeitail(symbol=self.trading_instrument, 
                                                                    productType="USDT-FUTURES", 
                                                                    orderId=self.bitget_close_limit_Id,
                                                                    clientOid=self.bitget_close_limit_Oid)
            #bitget_order_check =  self.bitget_master_orderApi_futures.detail(symbol=self.trading_instrument + "_UMCBL", orderId=self.bitget_close_stop_Id)
            
            self.close_helper_is_bitget_limit_filled = True if bitget_order_check['data']['state'] == 'filled' else False
            
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiontcheck bitget limit (closing)")
                print("---------------------------------------------")  
            # Check bitget limit
            
            bitget_close_stop_check = self.bitget_futures_orders_plan_pending(
                                                    orderId=self.bitget_close_stop_Id, 
                                                    clientOid='', 
                                                    symbol=self.trading_instrument,
                                                    planType='normal_plan',
                                                    productType='usdt-futures',
                                                    idLessThan='',
                                                    startTime='',
                                                    endTime='',
                                                    limit='')

            #bitget_close_limit_check =  self.bitget_master_orderApi_futures.detail(symbol=self.trading_instrument + "_UMCBL", orderId=self.bitget_close_limit_Id)
            
            self.close_helper_is_biget_stop_triggered  = False if bitget_close_stop_check['data']['entrustedList'][0]['planStatus'] == 'live' else True
            
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiontcheck bitget stop (closing)")
                print("---------------------------------------------")  

            # Check binacne
            binance_order_ckeck_limit =  self.binance_client_subaccount_futures.query_order(
                                                                symbol=self.trading_instrument,
                                                                orderId=self.binance_close_limit_Id,
                                                                origClientOrderId=self.binance_close_limit_client_Id )
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiontcheck binance limit (closing)")
                print("---------------------------------------------")  
            if binance_order_ckeck_limit['origQty'] == binance_order_ckeck_limit['executedQty']:
                self.close_helper_is_binance_limit_filled = True
            else:
                self.close_helper_is_binance_limit_filled = False
            # Check binance stop
            binance_order_ckeck =  self.binance_client_subaccount_futures.query_order(
                                                                            symbol=self.trading_instrument,
                                                                            orderId=self.binance_close_stop_Id,
                                                                            origClientOrderId=self.binance_close_stop_client_Id )
            if self.is_testing == True:
                print("---------------------------------------------")
                print("Checkpiontcheck binance stop (closing)")
                print("---------------------------------------------")  
            if binance_order_ckeck['origQty'] == binance_order_ckeck['executedQty']:
                self.close_helper_is_binance_stop_triggered = True
            else:
                self.close_helper_is_binance_stop_triggered = False            

            
            if  (self.close_helper_is_binance_limit_filled == True and 
                self.close_helper_is_bitget_limit_filled == True and 
                self.close_helper_is_biget_stop_triggered == False and
                self.close_helper_is_binance_stop_triggered == False):
                try:
                    self.binance_client_subaccount_futures.cancel_order(symbol=self.trading_instrument, 
                                                                        orderId=self.binance_close_stop_Id, 
                                                                        origClientOrderId=self.binance_close_stop_client_Id)
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiontcheck filled cancel binance stop (closing)")
                        print("---------------------------------------------")              
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                try:
                    bitget_data = self.bitget_futures_cancelTrigger(symbol=self.trading_instrument,
                                        productType="usdt-futures",
                                        orderIdList=[
                                            {
                                                "orderId": self.bitget_close_stop_Id,
                                                "clientOid": self.bitget_close_stop_Oid
                                            }
                                        ],
                                        marginCoin='USDT')
                except Exception as e:
                    print(e)
                self.opening_stage_1 = True
                self.closing_stage_2 = False
                self.mode_checkRecord = True
                self.mode_trade = False
                self.close_helper_is_binance_limit_filled = False 
                self.close_helper_is_bitget_limit_filled = False
                self.close_helper_is_biget_stop_triggered = False 
                self.close_helper_is_binance_stop_triggered = False
                return
            if  (self.close_helper_is_binance_limit_filled == False and 
                self.close_helper_is_bitget_limit_filled == True and 
                self.close_helper_is_biget_stop_triggered == False and
                self.close_helper_is_binance_stop_triggered == True):
                # Cancel bitget stop
                try:
                    bitget_data = self.bitget_futures_cancelTrigger(symbol=self.trading_instrument,
                                        productType="usdt-futures",
                                        orderIdList=[
                                            {
                                                "orderId": self.bitget_close_stop_Id,
                                                "clientOid": self.bitget_close_stop_Oid
                                            }
                                        ],
                                        marginCoin='USDT')
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiontcheck filled cancel bitget stop (closing)")
                        print("---------------------------------------------")              
                except Exception as error:
                    print(error)
                    return
                
                # Cancel binance
                try:
                    self.binance_client_subaccount_futures.cancel_order(symbol=self.trading_instrument, 
                                                                        orderId=self.binance_close_limit_Id, 
                                                                        origClientOrderId=self.binance_close_limit_client_Id)
                    if self.is_testing == True:
                        print("---------------------------------------------")
                        print("Checkpiontcheck filled cancel binance stop (closing)")
                        print("---------------------------------------------")              
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                
                self.opening_stage_1 = True
                self.closing_stage_2 = False
                self.mode_checkRecord = True
                self.mode_trade = False
                self.close_helper_is_binance_limit_filled = False 
                self.close_helper_is_bitget_limit_filled = False
                self.close_helper_is_biget_stop_triggered = False 
                self.close_helper_is_binance_stop_triggered = False
                return
            if  (self.close_helper_is_binance_limit_filled == True and 
                self.close_helper_is_bitget_limit_filled == False and 
                self.close_helper_is_biget_stop_triggered == True and
                self.close_helper_is_binance_stop_triggered == False):
                try:
                    self.binance_client_subaccount_futures.cancel_order(symbol=self.trading_instrument, 
                                                                        orderId=self.binance_close_stop_Id, 
                                                                        origClientOrderId=self.binance_close_stop_client_Id)
                except ClientError as error:
                    logging.error("Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message))
                    return
                
                # Cancel bitget limit
                try:
                    bitget_data = self.bitget_futures_cancelOrder(symbol=self.trading_instrument,
                                                                    productType="usdt-futures",
                                                                    orderId=self.bitget_close_limit_Id,
                                                                    clientOid=self.bitget_close_limit_Oid)
                except Exception as e:
                    print(e)
                if self.is_testing == True:
                    print("---------------------------------------------")
                    print("Checkpiontcheck bitget triggered cancel binance stop (closing)")
                    print("---------------------------------------------")

                self.opening_stage_1 = True
                self.closing_stage_2 = False
                self.mode_checkRecord = True
                self.mode_trade = False
                self.close_helper_is_binance_limit_filled = False 
                self.close_helper_is_bitget_limit_filled = False
                self.close_helper_is_biget_stop_triggered = False 
                self.close_helper_is_binance_stop_triggered = False
                return


    def write_record(self):
        """
            Record
        """

        print(f"Mode: Write Record")           

        binance_current_balance = self.get_balance(exchange='binance', type_='futures', account='subaccount', coin='USDT')
        bitget_currect_balance = self.get_balance(exchange='bitget', type_='futures', account='master', coin='USDT')

        binance_previous_balance = self.free_usdt_binance 
        bitget_previous_balance = self.free_usdt_bitget

        profit =  float(binance_current_balance) + float(bitget_currect_balance) - float(binance_previous_balance) - float(bitget_previous_balance)
        a = float(binance_current_balance) + float(bitget_currect_balance)
        b = float(binance_previous_balance) + float(bitget_previous_balance)
        percentage = self.get_change_percentage(a, b)


        ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = 'records.txt'
        record = {'timestamp': ct, 'symbol': self.trading_instrument, 'profit_usdt': profit, 'percentage':percentage}
        self.write_text_file(file_path, record)

        print(record)
        if self.is_testing == True: 
            print("---------------------------------------------")
            print("Checkpiontcheck bitget triggered cancel binance stop (closing)")
            print("---------------------------------------------")

        self.mode_check_record = False
        self.mode_get_balances = True

    def write_text_file(self, file_path, new_data):
    # Check if the file exists, if not create it with headers
        fieldnames = new_data.keys()

        # Check if the file exists, if not create it with headers
        if not os.path.isfile(file_path):
            with open(file_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
                writer.writeheader()

        # Append the new data to the existing file
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
            writer.writerow(new_data)



    ####################################################
    # Helpers
    def get_balance(self, exchange, type_, account, coin='USDT'):
        if coin != 'USDT':
            coin = coin.replace('USDT', '')
        # Binance
        # if exchange=='binance' and type_ == 'futures' and account =='subaccount':           # OK
        #     future_balances = self.binance_client_subaccount_futures.balance()
        #     return [x for x in future_balances if x['asset']==f'{coin}'][0]['balance']
        if exchange=='binance' and type_ == 'futures' and account =='subaccount':           # OK
            future_balances = self.binance_client_master_spot.sub_account_futures_account(email="arb_van_go_go_sub@outlook.com",  futuresType=1)
            return float(future_balances['data']['futureAccountResp']['totalWalletBalance'])
        
        if exchange=='binance' and type_ == 'futures' and account =='master':               # OK
            future_balances = self.binance_client_master_futures.balance()
            return [x for x in future_balances if x['asset']==f'{coin}'][0]['balance']
        
        if exchange=='binance' and type_ == 'spot' and account =='subaccount':              # OK         
            try:
                return self.binance_client_subaccount_spot.user_asset(asset=coin)['data'][0]['free']      
            except:
                return 0
        if exchange=='binance' and type_ == 'spot' and account =='master':                  # OK
            try:
                return self.binance_client_master_spot.user_asset(asset=coin)['data'][0]['free']
            except:
                return 0
        # Bitget
        if exchange=='bitget' and type_ == 'futures' and account =='subaccount':
            return self.bitget_subaccount_accountApi_futures.account(symbol='ETHUSDT'+'_UMCBL', marginCoin='USDT')['data']['available'] # OK
        
        if exchange=='bitget' and type_ == 'spot' and account =='subaccount':
            a = self.bitget_subaccount_accountApi_spot.assets(coin=coin.lower())['data'][0]['available']      # OK
            return a
        
        if exchange=='bitget' and type_ == 'futures' and account =='master':
            return self.bitget_master_accountApi_futures.account(symbol='ETHUSDT'+'_UMCBL', marginCoin='USDT')['data']['available'] # OK
        
        if exchange=='bitget' and type_ == 'spot' and account =='master':
            return self.bitget_master_accountApi_spot.assets(coin=coin.lower())['data'][0]['available']  # OK


    def transfer_withdrawal(self, exchange, from_, to_, coin, amount):
        
        # Binance

        if exchange == 'binance' and from_ == 'spot_subaccount' and to_ == 'spot_master':
            logging.info(self.binance_client_subaccount_spot.sub_account_transfer_to_master(asset=coin, amount=amount))           # OK 

        if exchange == 'binance' and from_ == 'spot_master' and to_ == 'spot_subaccount':
            logging.info(self.binance_client_master_spot.sub_account_universal_transfer(toEmail="arb_van_go_go_sub@outlook.com", 
                                                                                        fromAccountType="SPOT", toAccountType="SPOT", asset=coin, amount=amount))   # OK
            
        if exchange == 'binance' and from_ == 'spot_subaccount' and to_ == 'futures_subaccount':                                  # OK
            logging.info(self.binance_client_subaccount_spot.futures_transfer(asset=coin, amount=amount, type=1))

        if exchange == 'binance' and from_ == 'spot_master' and to_ == 'futures_master':                                          # OK
            logging.info(self.binance_client_master_spot.futures_transfer(asset=coin, amount=str(amount), type=1))

        if exchange == 'binance' and from_ == 'futures_subaccount' and to_ == 'spot_subaccount':                                 # OK
            logging.info(self.binance_client_subaccount_spot.futures_transfer(asset=coin, amount=amount, type=2))

        if exchange == 'binance' and from_ == 'futures_master' and to_ == 'spot_master':                                        # OK
            logging.info(self.bitget_master_walletApi_spot.transfer(asset=coin, amount=amount, type=2))

        # Bitget

        if exchange == 'bitget' and from_ == 'spot_master' and to_ == 'futures_master':                                        # OK
            logging.info(self.bitget_master_walletApi_spot.transfer( fromType='spot', toType='mix_usdt', amount=amount, coin=coin))

        if exchange == 'bitget' and from_ == 'futures_master' and to_ == 'spot_master':                                       # OK
            logging.info(self.bitget_master_walletApi_spot.transfer( fromType='mix_usdt', toType='spot', amount=amount, coin=coin))

        
        # Withdraeals
        if exchange=='binance' and from_=='spot_master' and  to_=='spot_master_bitget':
            logging.info(self.binance_client_master_spot.withdraw( coin='USDT', amount=self.amount_to_withdraw, address=self.bitget_usdt_adress))

        if exchange=='bitget' and from_=='spot_master' and  to_=='spot_master_binance':
            logging.info(self.bitget_master_walletApi_spot.withdrawal(coin='USDT', address=self.binance_usdt_adress, chain='ARBITRUMONE', amount=str(self.amount_to_withdraw), 
                                                                      remark='account offset', clientOid='5721424678'))


    def adjust_leverage(self, exchange, account, symbol, leverage):                               # OK
        if exchange == 'bitget' and account == 'master':
            logging.info(self.bitget_master_accountApi_futures.leverage(symbol=symbol +"_UMCBL", 
                                                                         marginCoin="USDT",
                                                                         leverage=leverage,
                                                                         ))
        if exchange == 'binance' and account == 'subaccount':                                     # OK
            logging.info(self.binance_client_subaccount_futures.change_leverage(symbol=symbol, 
                                                                         leverage=leverage,
                                                                         ))
            

    

    def bitget_futures_changeHoldMode(self, productType, holdMode):
        """
        https://bitgetlimited.github.io/apidoc/en/mix/#change-hold-mode
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = 'https://api.bitget.com/api/mix/v1/account/setPositionMode'
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/mix/v1/account/setPositionMode"
        # POST          @#umcbl                   single_hold
        params = {"productType":productType, 'holdMode':holdMode}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        print(response.status_code)
        print(response.json())

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

    def spread_calculator(self, data, amount):
        data = dict(list(data.items()))
        arb = dict( (x, {}) for x in ['sell_buy', 'buy_sell'])
        for key in [x for x in data.keys() if 'binance_futures' in x]:
            for key_2 in [x for x in data.keys() if 'bitget_futures' in x]:
                if key_2.split('_')[-1] == key.split('_')[-1]:
                    try:    
                        # Binance buy limit binance market bitget
                        # Close with bitget limit
                        a = self.slippage_calculator(depth=data[key], amount=amount, side='buy') 
                        b = self.slippage_calculator(depth=data[key_2], amount=amount, side='sell') # On practice, make the ask the closest to the price
                        result = self.get_change_percentage(a, b) 
                        ticker = key.split('_')[-1]
                        if ticker not in arb['sell_buy']:
                            arb['sell_buy'][ticker] = {}
                        arb['sell_buy'][ticker]['profit'] = result
                        arb['sell_buy'][ticker]['binance_ask'] = data[key]['asks'][0][0]
                        arb['sell_buy'][ticker]['binance_bid'] = data[key]['bids'][0][0]
                        arb['sell_buy'][ticker]['bitget_ask'] = data[key_2]['asks'][0][0]
                        arb['sell_buy'][ticker]['bitget_bid'] = data[key_2]['bids'][0][0]

                        # Binance buy limit bitget market binance
                        # Close with bitget limit
                        a = self.slippage_calculator(depth=data[key], amount=amount, side='sell') 
                        b = self.slippage_calculator(depth=data[key_2], amount=amount, side='buy') # On practice, make the ask the closest to the price
                        result = self.get_change_percentage(b, a) 
                        ticker = key.split('_')[-1]
                        if ticker not in arb['buy_sell']:
                            arb['buy_sell'][ticker] = {}
                        arb['buy_sell'][ticker]['profit'] = result
                        arb['buy_sell'][ticker]['binance_ask'] = data[key]['asks'][0][0]
                        arb['buy_sell'][ticker]['binance_bid'] = data[key]['bids'][0][0]
                        arb['buy_sell'][ticker]['bitget_ask'] = data[key_2]['asks'][0][0]
                        arb['buy_sell'][ticker]['bitget_bid'] = data[key_2]['bids'][0][0]

                    except:
                        pass
        return arb

  
    def spreads_calculator_books(self, spreads):

        all = {}

        for strategie in spreads.keys():

            profits = {ticker: spreads[strategie][ticker]['profit'] for ticker in spreads[strategie].keys()}

            sorted_items = sorted(profits.items(), key=lambda item: item[1], reverse=False)
            sorted_dict = dict(sorted_items)
            keys_BS = list(sorted_dict.keys())
            last_key_BS = keys_BS[::-1][:10]

            sorted_profits = {x:sorted_dict[x] for x in last_key_BS}

            full = {}

            for key, value in sorted_profits.items():
                full[key] = {}
                full[key]['profit'] = value
                full[key]['binance_ask'] =  spreads[strategie][key]['binance_ask']
                full[key]['binance_bid'] =  spreads[strategie][key]['binance_bid']
                full[key]['bitget_ask'] =  spreads[strategie][key]['bitget_ask']
                full[key]['bitget_bid'] =  spreads[strategie][key]['bitget_bid']

            all[strategie] = full
        flattened_data = []

        for strategie, assets in all.items():
            for asset, info in assets.items():
                info['strategie'] = strategie
                flattened_data.append((asset, info))
        sorted_data = sorted(flattened_data, key=lambda x: x[1]['profit'], reverse=True)


        result = {asset: info for asset, info in sorted_data}
    
        return result

    def sort_dictionary_by_values_books(self, dictionary):
        sorted_items = sorted(dictionary.items(), key=lambda item: item[1], reverse=False)
        sorted_dict = dict(sorted_items)
        return sorted_dict
    
    def adjust_ALL_binance_leverage(self, leverage):
        binance_futures_tickers = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        binance_futures_tickers = binance_futures_tickers.json()
        binance_futures_tickers = [item['symbol'] for item in binance_futures_tickers['symbols']]
        # Bitget futures
        bitget_futures_tickers = [e['symbol'].split('_')[0] for e in json.loads(requests.get("https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl").text)['data']]
        # Discard unusable tickers
        ff = [x for x in binance_futures_tickers if x in bitget_futures_tickers]
        futures_tickers = ff 
        for symbol in futures_tickers:
            self.adjust_leverage(exchange='binance', account='subaccount', symbol=symbol, leverage=leverage)
            print(f'Leverage on {symbol} on binance is set')
            time.sleep(1)

        print('Leverage on binance futures adjusted')


    def set_positionHoldMode_binance(self):
          try:
              response = self.binance_client_subaccount_futures.change_position_mode(
                  dualSidePosition="false", recvWindow=2000
              )
              logging.info(response)
          except ClientError as error:
              logging.error(
                  "Found error. status: {}, error code: {}, error message: {}".format(
                      error.status_code, error.error_code, error.error_message
                  )
              )


    def bitget_futures_changePosMode(self, productType, posMode):
        """
        https://www.bitget.com/api-doc/contract/account/Change-Hold-Mode
        # bot.bitget_futures_changePosMode('USDT-FUTURES', 'one_way_mode' hedge_mode )
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = 'https://api.bitget.com/api/v2/mix/account/set-position-mode'
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/account/set-position-mode"
        # POST          @#umcbl                   single_hold
        params = {"productType":productType, 'posMode':posMode}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        print(response.status_code)
        print(response.json())


    def bitget_futures_palceOrder(self, symbol, productType, marginMode, marginCoin, size, price, side, 
                                  tradeSide, orderType, force,  reduceOnly, presetStopSurplusPrice, presetStopLossPrice, clientOid):
        """
        # https://www.bitget.com/api-doc/contract/trade/Place-Order
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = 'https://api.bitget.com/api/v2/mix/order/place-order'
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/place-order"
        # POST          @#umcbl                   single_hold
        params = {"symbol":symbol, "productType":productType, "marginMode":marginMode, "marginCoin":marginCoin, 
                  "size":size, "price":price, "side":side, 
                    "tradeSide":tradeSide, "orderType":orderType, "force":force, 
                    "reduceOnly":reduceOnly, 
                    "presetStopSurplusPrice":presetStopSurplusPrice, "presetStopLossPrice":presetStopLossPrice, 'clientOid':clientOid}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        print(response.json())
        return response.json()


    def bitget_futures_modifyOrder(self, orderId, clientOid, symbol, newClientOid, newPresetStopLossPrice, productType):
        """
        https://bitgetlimited.github.io/apidoc/en/mix/#modify-order
        """
        def get_timestamp():
            return int(time.time() * 1000)


        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)


        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body


        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'

            return url[0:-1]
        
                
        base_url = "https://api.bitget.com/api/v2/mix/order/modify-order"
        API_SECRET_KEY = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/modify-order"
        # POST          @#umcbl                   single_hold
        params = {"orderId":orderId, "symbol":symbol, "newClientOid":newClientOid, "clientOid":clientOid, "newPresetStopLossPrice":newPresetStopLossPrice, "productType":productType}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), API_SECRET_KEY)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        print(response.status_code)
        print(response.json())
        return response.json()


    def bitget_futures_cancelOrder(self, symbol, productType, orderId, clientOid):
        """
        https://www.bitget.com/api-doc/contract/trade/Cancel-Order
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = "https://api.bitget.com/api/v2/mix/order/cancel-order"
        API_SECRET_KEY = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/cancel-order"
        # POST          @#umcbl                   single_hold
        params = {"symbol":symbol, "productType":productType, "orderId":orderId, "clientOid":clientOid}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), API_SECRET_KEY)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        #print(response.status_code)
        #print(response.json())
        return response.json()


    def bitget_futures_cancelTrigger(self, 
                                     orderIdList, 
                                     symbol, 
                                     productType, 
                                     marginCoin):
        """
        https://www.bitget.com/api-doc/contract/plan/Cancel-Plan-Order
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = "https://api.bitget.com/api/v2/mix/order/cancel-plan-order"
        API_SECRET_KEY = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/cancel-plan-order"
        # POST          @#umcbl                   single_hold
        params = {"orderIdList":orderIdList, 
                  "symbol":symbol,
                  "productType":productType,
                  "marginCoin":marginCoin}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), API_SECRET_KEY)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        #print(response.status_code)
        #print(response.json())
        return response.json()

    def bitget_futures_TriggerOrder(self, 
                                    planType, 
                                    symbol, 
                                    productType,
                                    marginMode,
                                    marginCoin,
                                    size,
                                    price,
                                    callbackRatio,
                                    triggerPrice,
                                    triggerType,
                                    side,
                                    tradeSide,
                                    orderType,
                                    clientOid,
                                    reduceOnly,
                                    stopSurplusTriggerPrice,
                                    stopSurplusExecutePrice,
                                    stopSurplusTriggerType,
                                    stopLossTriggerPrice,
                                    stopLossExecutePrice,
                                    stopLossTriggerType):
        """
        https://www.bitget.com/api-doc/contract/plan/Place-Tpsl-Order
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = "https://api.bitget.com/api/v2/mix/order/place-plan-order"
        API_SECRET_KEY = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/place-plan-order"
        # POST          @#umcbl                   single_hold
        params = {"planType":planType, 
                  "symbol":symbol, 
                  "productType":productType, 
                  "marginMode":marginMode,
                  "marginCoin":marginCoin,
                  "size":size,
                  "price":price,
                  "callbackRatio":callbackRatio,
                  "size":size,
                  "triggerPrice":triggerPrice,
                  "triggerType":triggerType,
                  "side":side,
                  "tradeSide":tradeSide,
                  "orderType":orderType,
                  "clientOid":clientOid,
                  "reduceOnly":reduceOnly,
                  "stopSurplusTriggerPrice":stopSurplusTriggerPrice,
                  "stopSurplusExecutePrice":stopSurplusExecutePrice,
                  "stopSurplusTriggerType":stopSurplusTriggerType,
                  "stopLossTriggerPrice":stopLossTriggerPrice,
                  "stopLossExecutePrice":stopLossExecutePrice,
                  "stopLossTriggerType":stopLossTriggerType,
                  }
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), API_SECRET_KEY)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        #print(response.status_code)
        #print(response.json())
        return response.json()

    def bitget_futures_orders_plan_pending(self, 
                                    orderId, 
                                    clientOid, 
                                    symbol,
                                    planType,
                                    productType,
                                    idLessThan,
                                    startTime,
                                    endTime,
                                    limit
                                    ):
        """
        https://www.bitget.com/api-doc/contract/plan/get-orders-plan-pending
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = "https://api.bitget.com/api/v2/mix/order/orders-plan-pending"
        API_SECRET_KEY = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/orders-plan-pending"
        # POST          @#umcbl                   single_hold
        params = {"orderId":orderId, 
                  "clientOid":clientOid, 
                  "symbol":symbol, 
                  "planType":planType,
                  "productType":productType,
                  "idLessThan":idLessThan,
                  "startTime":startTime,
                  "endTime":endTime,
                  "limit":limit
                  }
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        base_url = base_url + parse_params_to_str(params)
        request_path = request_path + parse_params_to_str(params)
        sign_ = sign(pre_hash(timestamp, "GET", request_path, str(body)), API_SECRET_KEY)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.get(base_url, data=body, headers=headers)
        #print(response.status_code)
        #print(response.json())
        return response.json()




    def bitget_futures_change_leverage(self, symbol, productType, marginCoin, leverage):
        """
        https://www.bitget.com/api-doc/contract/account/Change-Leverage
        """
        def get_timestamp():
            return int(time.time() * 1000)
        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)
        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body
        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'
            return url[0:-1]
        
        base_url = 'https://api.bitget.com/api/v2/mix/account/set-leverage'
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/account/set-leverage"
        # POST          @#umcbl                   single_hold
        params = {"symbol":symbol, "productType":productType, "leverage":leverage, "marginCoin":marginCoin}
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        print(response.status_code)
        print(response.json())

    def bitget_futures_change_leverage_ALL(self, leverage):
        binance_futures_tickers = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        binance_futures_tickers = binance_futures_tickers.json()
        binance_futures_tickers = [item['symbol'] for item in binance_futures_tickers['symbols']]
        # Bitget futures
        bitget_futures_tickers = [e['symbol'].split('_')[0] for e in json.loads(requests.get("https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl").text)['data']]
        # Discard unusable tickers
        ff = [x for x in binance_futures_tickers if x in bitget_futures_tickers]
        futures_tickers = ff 
        print(futures_tickers)
        for symbol in futures_tickers:
            self.bitget_futures_change_leverage( symbol=symbol.lower(), productType="USDT-FUTURES", marginCoin='USDT', leverage=str(leverage))
            print(f'{symbol} leverage is set on bitget')
            time.sleep(0.5)
        print('All coins levereges are set')


    def bitget_futures_orderDeitail(self, symbol, productType, orderId, clientOid):
        """
        https://www.bitget.com/api-doc/contract/trade/Get-Order-Details
        """
        def get_timestamp():
            return int(time.time() * 1000)


        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)


        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body


        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'

            return url[0:-1]
            
        base_url = "https://api.bitget.com/api/v2/mix/order/detail"
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/detail"
        
        # POST          @#umcbl                   single_hold
        params = {"symbol":symbol.lower(), "productType":productType, "orderId":orderId, "clientOid":clientOid}


        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        base_url = base_url + parse_params_to_str(params)
        request_path = request_path + parse_params_to_str(params)
        print(base_url)
        sign_ = sign(pre_hash(timestamp, "GET", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.get(base_url, data=body, headers=headers)
        #print(response.status_code)
        #print(response.json())
        return response.json()

    def bitget_futures_pendingOrders(self, symbol, productType, orderId, clientOid):
        """
        https://www.bitget.com/api-doc/contract/trade/Get-Order-Details
        """
        def get_timestamp():
            return int(time.time() * 1000)


        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)


        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body


        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'

            return url[0:-1]
        
        base_url = 'https://api.bitget.com/api/v2/mix/order/orders-pending'
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/mix/order/orders-pending"
        # POST          @#umcbl                   single_hold
        params = {"symbol":symbol, "productType":productType, "orderId":orderId, "clientOid":clientOid}
        #request_path = request_path + parse_params_to_str(params)
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "GET", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.get(base_url, data=body, headers=headers)
        #print(response.status_code)
        #print(response.json())
        return response.json()

    def bitget_futures_withdrawal(self, coin, transferType, address, chain, size):
        """
        https://www.bitget.com/api-doc/contract/trade/Get-Order-Details
        """
        def get_timestamp():
            return int(time.time() * 1000)


        def sign(message, secret_key):
            mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
            d = mac.digest()
            return base64.b64encode(d)


        def pre_hash(timestamp, method, request_path, body):
            return str(timestamp) + str.upper(method) + request_path + body


        def parse_params_to_str(params):
            url = '?'
            for key, value in params.items():
                url = url + str(key) + '=' + str(value) + '&'

            return url[0:-1]
        
        base_url = 'https://api.bitget.com/api/v2/spot/wallet/withdrawal'
        secret = self.bitget_master_secret
        api =  self.bitget_master_api
        password = self.bitet_master_password
        timestamp =  get_timestamp()
        request_path = "/api/v2/spot/wallet/withdrawal"
        # POST          @#umcbl                   single_hold
        params = {"coin":coin, "transferType":transferType, "address":address, "chain":chain, "size":size}
        #request_path = request_path + parse_params_to_str(params)
        body = json.dumps(params)
        #print(pre_hash(timestamp, "POST", request_path, str(body)))
        sign_ = sign(pre_hash(timestamp, "POST", request_path, str(body)), secret)
        headers = {
                'ACCESS-KEY': api,
                'ACCESS-SIGN': sign_,
                'ACCESS-TIMESTAMP': str(timestamp),
                'Content-Type': 'application/json',
                'ACCESS-PASSPHRASE': password,
            }
        response = requests.post(base_url, data=body, headers=headers)
        print(response.status_code)
        print(response.json())
        return response.json()
