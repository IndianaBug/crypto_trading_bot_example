import pandas as pd
import numpy as np
from collections import Counter
import json
import sqlite3
import csv
import numpy as np

def analzyze_percentage(data_arbitrage, strategy, fee_break_even_rate):

    # Timer
    count = len(list(data_arbitrage.columns))
    print('Left to do')
    # Target variables
    percentage_range = [0.1, 0.2, 0.3]
    spread_time = [1, 2, 5, 10, 30, 60, 600, 3600]
    decay_range = [60, 60*10, 60*60, 60*60*5]

    # Spread opportunity duration lookup 
    spreads_second = {}
    for percentage in percentage_range:
        spreads_second[percentage] = {}
        for second in spread_time:
            spreads_second[percentage][second] = 0
    # Spread opportunity decay lookup per duration
    decay_second = {}
    for percentage in percentage_range:
        decay_second[percentage] = {}
        for second in spread_time:
            decay_second[percentage][second] = {}
            for decay in decay_range:
                decay_second[percentage][second][decay] = 0

    print(spreads_second)
    for index_column, column in zip(range(len(data_arbitrage.columns)), list(data_arbitrage.columns)):
        # Tme tracker
        print(count)
        count -= 1
        #print(spreads_second)
        #print(decay_second)
        for index_cell in range(0, len(data_arbitrage)):
            for second in spread_time:
                try:
                    # Current cell value
                    cell_value = data_arbitrage.iloc[index_cell, index_column]
                    try:
                        previous_cell = data_arbitrage.iloc[index_cell-1, index_column]
                    except:
                        previous_cell = 0
                    try:
                        # Gets the last cell that will be analyzed
                        ending_cell = data_arbitrage.iloc[second+index_cell, index_column]
                    except:
                        ending_cell = 0
                    
                    # Count duration of entries
                    for index_percentage, percentage in enumerate(percentage_range):
                        if index_percentage != len(percentage_range)-1:
                            if cell_value >= percentage and cell_value < percentage_range[index_percentage+1] and previous_cell < cell_value and ending_cell < percentage:
                                # Counts durations
                                if (data_arbitrage.iloc[index_cell:second+index_cell, index_column] <= cell_value).all() and len(data_arbitrage) - index_cell > second:
                                    #print('ok')
                                    spreads_second[percentage][second] += 1
                                    #Counts decays
                                    for idnex_decay, decay in enumerate(decay_range):
                                        if idnex_decay != len(percentage_range)-1 and len(data_arbitrage) - index_cell > decay_range[idnex_decay+1]:
                                            if idnex_decay == 0:
                                                if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any():
                                                    decay_second[percentage][second][decay] += 1
                                            if idnex_decay != 0:
                                                if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any() and (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay_range[idnex_decay-1]+1, index_column] > fee_break_even_rate).all():
                                                    decay_second[percentage][second][decay] += 1
                                        if idnex_decay == len(percentage_range)-1 and len(data_arbitrage) - index_cell > decay_range[idnex_decay+1]:
                                            if (data_arbitrage.iloc[index_cell+second+1:, index_column] < fee_break_even_rate).any():
                                                if idnex_decay == 0:
                                                    if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any():
                                                        decay_second[percentage][second][decay] += 1
                                                if idnex_decay != 0:
                                                    if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any() and (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay_range[idnex_decay-1]+1, index_column] > fee_break_even_rate).all():
                                                        decay_second[percentage][second][decay] += 1
                        # Another constrain
                        if index_percentage == len(percentage_range)-1:
                            if cell_value >= percentage  and previous_cell < percentage and ending_cell < percentage:
                                if (data_arbitrage.iloc[index_cell:second+index_cell, index_column] <= cell_value).all() and len(data_arbitrage) - index_cell > second:
                                    spreads_second[percentage][second] += 1
                                    # Counts decays
                                    for idnex_decay, decay in enumerate(decay_range):
                                        if idnex_decay != len(percentage_range)-1 and len(data_arbitrage) - index_cell > decay_range[idnex_decay+1]:
                                            if idnex_decay == 0:
                                                if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any():
                                                    decay_second[percentage][second][decay] += 1
                                            if idnex_decay != 0:
                                                if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any() and (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay_range[idnex_decay-1]+1, index_column] > fee_break_even_rate).all():
                                                    decay_second[percentage][second][decay] += 1
                                        if idnex_decay == len(percentage_range)-1 and len(data_arbitrage) - index_cell > decay:
                                            if idnex_decay == 0:
                                                if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any():
                                                    decay_second[percentage][second][decay] += 1
                                            if idnex_decay != 0:
                                                if (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay+1, index_column] < fee_break_even_rate).any() and (data_arbitrage.iloc[index_cell+second+1:second+index_cell+decay_range[idnex_decay-1]+1, index_column] > fee_break_even_rate).all():
                                                    decay_second[percentage][second][decay] += 1
                except:
                  pass                    
    
    print('Congratulations!!!')

    with open(f"analyzer_results/spreads_second_{strategy}.json", "w") as json_file:
        json.dump(spreads_second, json_file)

    with open(f"analyzer_results/decay_second_{strategy}.json", "w") as json_file:
        json.dump(decay_second, json_file)


strategies = ['FF_lm_bs', 'FF_lm_sb', 'FF_mm_bs', 'FF_mm_sb',
              'FS_lm_bs', 'FS_lm_sb', 'FS_mm_bs', 'FS_mm_sb',
              'SF_lm_bs', 'SF_lm_sb', 'SF_mm_bs', 'SF_mm_sb',
              'SS_lm_bs', 'SS_lm_sb', 'SS_mm_bs', 'SS_mm_sb']

fee_break_even_rates = [-0.2, -0.2, -0.28, -0.28,
                        -0.36, -0.36, -0.42, -0.42
                        -0.42, -0.42, -0.42, -0.42,
                        -0.5, -0.5, -0.5, -0.5]

for strategie, fee_break_even_rate in zip(strategies, fee_break_even_rates):
    arbitrage_path = r"C:/coding/neutral_trading_bot/database/ticks.db"
    db_connection = sqlite3.connect(arbitrage_path)
    command_1 = f"""
                SELECT * FROM {strategie}
                LIMIT 21500
                """
    data_arbitrage_ = pd.read_sql_query(command_1, db_connection)
    db_connection.close()
    if strategies == 'FF_mm_bs' or 'FF_mm_sb':
        for column in data_arbitrage_.columns:
            if column != 'timestamp':
                data_arbitrage_[column] = data_arbitrage_[column] + 0.08
    


    analzyze_percentage(data_arbitrage_, strategie, fee_break_even_rate=fee_break_even_rate)




