import numpy as np

def stream_slippage_calculator(depth, amount, side):
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



def difference_percentages(current, previous):
    if current == previous:
        return 0
    else:
        return ((current - previous) / previous) * 100.0

def streaming_spread_calculator(data, amount):
    data = dict(list(data.items()))
    arb = dict( (x, {}) for x in ['sell_buy', 'buy_sell'])
    for key in [x for x in data.keys() if 'binance_futures' in x]:
        for key_2 in [x for x in data.keys() if 'bitget_futures' in x]:
            if key_2.split('_')[-1] == key.split('_')[-1]:
                try:    
                    # Binance buy limit binance market bitget
                    # Close with bitget limit
                    a = stream_slippage_calculator(depth=data[key], amount=amount, side='buy') 
                    b = stream_slippage_calculator(depth=data[key_2], amount=amount, side='sell') # On practice, make the ask the closest to the price
                    result = difference_percentages(a, b) 
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
                    a = stream_slippage_calculator(depth=data[key], amount=amount, side='sell') 
                    b = stream_slippage_calculator(depth=data[key_2], amount=amount, side='buy') # On practice, make the ask the closest to the price
                    result = difference_percentages(b, a) 
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