import asyncio
from aiokafka import AIOKafkaConsumer
import json
from neutral_trade_executor import Executor
latest_message = None

apis = {"binance_master_api" : "zWMs9P8CayaffAKQkJpjPMREe0gTlEvSu48dRUt9efMIKX9wXCdoTob4yF0NEzyT",
        "binance_master_secret" : "AOCXLapVPE0jLya0eHwz63UK9wt2QYHKdzN4l62wg45jzsbCJqkiDKH9TgAxuUfu",
        "binance_subaccount_api" : "AsjLVroTtvwm2nWOgdz5wAVAwfkb17OKbXpcWBgBrQAiO7VWVdL45wIoChkYiHOX",
        "binance_subaccount_secret" : "uTnxfGbkBnnSrVhDKvymOoCwSbECDrr8OLhyr4unaMMd9UiiSS8K7YgeVbaXwhYu",
        "exchange_master_api" : 'bg_95a8562ae59facd339935b8927102b36',
        "exchange_master_secret" : 'dadbddce552dad54159c1b04c10065e5086a0eb981d359b4a22ffd046ca2d7ea',
        "exchange_subaccount_api" : 'bg_8c398a0451bddf4296a786b57cbd5dcf',
        "exchange_subaccount_secret" : 'a11a5d5249e58c2fbedcb6b14eef289b73365db848c2cca811fe34f6f504b78a',
        "bitget_master_password" : 'ENKHhHgq9tmdBu1265',
        "bitget_subaccount_password" : '3OhtYz3LcxCC',
        "bitget_master_ID": "5721424678",
        "bitget_subaccount_ID": "8010474081",
        "bitget_master_withdrawal_pass": "E$fEm6JkYNcVfem"
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
# # Do this everytime you turn on the bot
# bot.bitget_futures_change_leverage_ALL(leverage=1)
# bot.bitget_futures_changePosMode(productType='USDT-FUTURES', posMode='one_way_mode')
# bot.set_positionHoldMode_binance()
# bot.adjust_ALL_binance_leverage(leverage=1)

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





