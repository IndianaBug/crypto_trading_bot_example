import asyncio
from aiokafka import AIOKafkaConsumer
import json
from neutral_trade_executor import Executor
latest_message = None

apis = {"binance_master_api" : "",
        "binance_master_secret" : "",
        "binance_subaccount_api" : "",
        "binance_subaccount_secret" : "",
        "exchange_master_api" : '',
        "exchange_master_secret" : '',
        "exchange_subaccount_api" : '',
        "exchange_subaccount_secret" : '',
        "bitget_master_password" : '',
        "bitget_subaccount_password" : '',
        "bitget_master_ID": "",
        "bitget_subaccount_ID": "",
        "bitget_master_withdrawal_pass": ""
        }

bot = Executor(apis=apis, 
               amount_percentage_to_trade=0.05, 
               open_spread_percentage=0.18, 
               open_close_spread_difference=0.02, 
               transfer_difference=30, 
               stop_percentage=0.008,
               spread_expansion=0.0000, 
               is_testing=True
               )
# Do this everytime you turn on the bot
bot.bitget_futures_change_leverage_ALL(leverage=1)
bot.bitget_futures_changePosMode(productType='USDT-FUTURES', posMode='one_way_mode')
bot.set_positionHoldMode_binance()
bot.adjust_ALL_binance_leverage(leverage=1)

async def consume():
    global latest_message
    consumer = AIOKafkaConsumer(
        'spreads',  
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=False)
    await consumer.start()

    async for message in consumer:
        try:
            latest_message = message.value.decode('utf-8')
            bot.main(json.loads(latest_message))
        except:
            continue

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(consume())





