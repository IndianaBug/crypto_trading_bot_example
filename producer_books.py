import websockets
import asyncio
import time
import json
import requests
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaStorageError
from collections import Counter
import ssl



ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

class WebSocketClient():

    def __init__ (self, host, **kwards):
        
        self.host = host

        # Binance futures
        self.binance_futures_tickers = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        self.binance_futures_tickers = self.binance_futures_tickers.json()
        self.binance_futures_tickers = [item['symbol'] for item in self.binance_futures_tickers['symbols']]
        # Bitget futures
        self.bitget_futures_stream = "wss://ws.bitget.com/v2/ws/public"
        self.bitget_futures_tickers = [e['symbol'].split('_')[0] for e in json.loads(requests.get("https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl").text)['data']]
        # Binance spot
        self.binance_spot_tickers = requests.get("https://api.binance.com/api/v3/exchangeInfo")
        self.binance_spot_tickers = self.binance_spot_tickers.json()
        self.binance_spot_tickers = [item['symbol'] for item in self.binance_spot_tickers['symbols']]
        # Bitget spot
        self.bitget_spot_stream = "wss://ws.bitget.com/spot/v1/stream"
        self.bitget_spot_tickers = [e['symbol'].split('_')[0] for e in json.loads(requests.get("https://api.bitget.com/api/spot/v1/public/products").text)['data']]
        # Discard unusable tickers
        ff = [x for x in self.binance_futures_tickers if x in self.bitget_futures_tickers]
        self.futures_tickers = ff 
        self.binance_futures_authenticators = [f"wss://fstream.binance.com/ws/{symbol}@depth20@500ms" for symbol in self.futures_tickers]
        self.bitget_futures_authenticators = json.loads(f"""{{"op": "subscribe", "args": []}}""")
        for ticker in self.futures_tickers:
            self.bitget_futures_authenticators["args"].append(json.loads(f"""{{"instType": "USDT-FUTURES", "channel": "books15", "instId": "{ticker}"}}"""))
        self.bitget_futures_authenticators = json.dumps(self.bitget_futures_authenticators)

    
    async def keep_alive_bitget(self, websocket, ping_interval=30):
        while True:
            try:
                # Send a ping message to the server.
                await asyncio.sleep(ping_interval)
                await websocket.send('ping')
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed. Stopping keep-alive.")
                break
    
    async def keep_alive_binance(self, websocket, ping_interval=30):
        while True:
            try:
                # Send a ping message to the server.
                await websocket.pong()
                await asyncio.sleep(ping_interval)
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed. Stopping keep-alive.")
                break

    async def bitget_websockets(self, bitget_stream, data_auth, producer, topic):
        async for websocket in websockets.connect(bitget_stream, ping_interval=None, timeout=86400, ssl=ssl_context): #as websocket:
            await websocket.send(data_auth)
            keep_alive_task = asyncio.create_task(self.keep_alive_bitget(websocket, 30))
            try:
                async for message in websocket:
                    #keep_alive_task = asyncio.create_task(self.keep_alive_bitget(websocket, 30))
                    try:
                        message = await websocket.recv()
                        await producer.send_and_wait(topic=topic, value=str(message).encode())
                        print(f'Bitget_{topic} recieved')
                    except KafkaStorageError as e:
                        print(f"KafkaStorageError: {e}")
                        await asyncio.sleep(5)
                        continue
            except asyncio.exceptions.TimeoutError:
                print("WebSocket operation timed out")
                await asyncio.sleep(5)
                continue
            except websockets.exceptions.ConnectionClosed:
                print("connection  closed of bitget stream, reconnecting!!!")
                await asyncio.sleep(5)
                continue
        

    async def binance_websockets(self, binance_stream, producer, topic):
        ticker = binance_stream.split('/')[-1].split('@')[0]
        async for websocket in websockets.connect(binance_stream, ping_interval=None, timeout=86400):
            keep_alive_task = asyncio.create_task(self.keep_alive_binance(websocket, 30))
            try:
                async for message in websocket:
                    try:
                        if topic == 'binance_spot':
                            await producer.send_and_wait(topic=topic, value=json.dumps({ticker: json.loads(message)}).encode())
                        else:
                            await producer.send_and_wait(topic=topic, value=str(message).encode())
                        #print(json.dumps({ticker: json.loads(message)}).encode())
                        print(f'Binance_{topic} recieved')
                    except KafkaStorageError as e:
                        print(f"KafkaStorageError: {e}")
            except asyncio.exceptions.TimeoutError:
                await asyncio.sleep(5)
                print("WebSocket operation timed out")
            except websockets.exceptions.ConnectionClosed:
                print("connection  closed of binance stream, reconnecting!!!")
                await asyncio.sleep(5)
                continue

    async def main(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.host)
        await producer.start()
        #producer = ''
        tasks = []
        tasks += [self.binance_websockets(stream.lower(), producer, 'binance') for stream in self.binance_futures_authenticators]
        tasks +=  [self.bitget_websockets(bitget_stream=self.bitget_futures_stream, data_auth=self.bitget_futures_authenticators, producer=producer, topic='bitget')]
        try:
            await asyncio.gather(*tasks)
        finally:
            await producer.stop()

if __name__ == '__main__':
    client = WebSocketClient(host='localhost:9092')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.main())




