import faust
import asyncio
import json
import datetime
from aiokafka import AIOKafkaProducer
import time
import requests
import numpy as np
from database_handler import DatabaseHandler
from datetime import datetime
from spread_calculator import streaming_spread_calculator
import numpy as np


#db = DatabaseHandler(abs_path=r"database/ticks.db")
#db.create_table_prices()
#db.insert_columns()


app = faust.App('arbitrage_streaming_5',  
                broker='kafka://localhost:9092', 
                value_serializer='raw', 
                partitions = 2,
                broker_max_poll_records = 20000,
                consumer_auto_offset_reset='earliest')#, retention = 60000)

all_prices = app.Table('all_prices', default=int, partitions=2)
binance_futures_topic = app.topic('binance', partitions = 2, internal=False) #, internal=True)
#binance_spot_topic = app.topic('binance_spot', partitions = 3, internal=False, retention = 60000) #, internal=True)
bitget_futures_topic = app.topic('bitget', partitions = 2, internal=False) #, internal=True)
#bitget_spot_topic = app.topic('bitget_spot', partitions = 3, internal=False, retention = 60000) #, internal=True)

@app.agent(binance_futures_topic)
async def process_binance_futures(ticks): 
    async for tick in ticks:
        try:
            tick = json.loads(tick.decode('utf-8'))
            #print(tick)
            all_prices[f"binance_futures_{tick['s']}"] = {}
            all_prices[f"binance_futures_{tick['s']}"]['asks'] = tick['a']
            all_prices[f"binance_futures_{tick['s']}"]['bids'] = tick['b']
        except:
            continue


@app.agent(bitget_futures_topic)
async def process_bitget_futures(ticks): 
    async for tick in ticks:
        try:
            tick = json.loads(tick.decode('utf-8'))
            #print(tick)
            all_prices[f"bitget_futures_{tick['arg']['instId']}"] = {}
            all_prices[f"bitget_futures_{tick['arg']['instId']}"]['asks'] = tick['data'][0]['asks']
            all_prices[f"bitget_futures_{tick['arg']['instId']}"]['bids'] = tick['data'][0]['bids']
        except:
            continue


async def send_message(spreads, topic):
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',  
    )
    await producer.start()

    try:
        await producer.send_and_wait(topic=topic, value=str(spreads).encode())
        print('message is sent')
        print(spreads)

    finally:
        await producer.stop()





@app.timer(interval=1)
async def main():

    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    data = dict(list(all_prices.items()))
    data['timestamp'] = formatted_date
    # with open('lates_data.json', 'w') as json_file:
    #     json.dump(data, json_file)
    #profits = db.estimated_profit_calculator(data, amount = 1000)
    
    #db.insert_many(profits)

    spreads = streaming_spread_calculator(data=data, amount=10)

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(send_message('message', spreads))
    await send_message(json.dumps(spreads), 'spreads')

    with open('lates_data.json', 'w') as json_file:
        json.dump(spreads, json_file)


