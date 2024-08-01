
# import json
# import websocket
# import os
# from confluent_kafka import Producer
# import time

# API_KEY = os.environ['API_KEY']

# producer = Producer({'bootstrap.servers': 'kafka-container:9092'})

# def delivery_callback(err, msg):
#     if err:
#         print('ERROR: Message failed delivery: {}'.format(err))
#     else:
#         print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

# while True:
#     data = {"data":[{"c":None,"p":63229.69,"s":"BINANCE:BTCUSDT","t":1719893836373,"v":0.00139},{"c":None,"p":63229.69,"s":"BINANCE:BTCUSDT","t":1719893836373,"v":0.00184},{"c":None,"p":63229.7,"s":"BINANCE:BTCUSDT","t":1719893836887,"v":0.0001},{"c":None,"p":63229.7,"s":"BINANCE:BTCUSDT","t":1719893836887,"v":0.0001},{"c":None,"p":63229.7,"s":"BINANCE:BTCUSDT","t":1719893836887,"v":0.0001},{"c":None,"p":63229.7,"s":"BINANCE:BTCUSDT","t":1719893836887,"v":0.00142}],"type":"trade"}
#     data_str = json.dumps(data)
#     producer.produce('stock-trades', data_str, 'AAPL', callback=delivery_callback)
#     producer.poll(0)
#     time.sleep(1)


import json
import time
import websocket
import os
from confluent_kafka import Producer
from datetime import datetime


API_KEY = os.environ['API_KEY']

producer = Producer({'bootstrap.servers': 'kafka-container:9092'})

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: value = {value:12}".format(topic=msg.topic(), value=msg.value().decode('utf-8')))

def on_message(ws, message):
    producer.produce('stock-trades', message, callback=delivery_callback)
    producer.poll(0)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")
    producer.flush()
    producer.close()

def on_open(ws):
    with open('tickers.txt', 'r') as f:
        for line in f:
            message_json = {'type':'subscribe','symbol':line.strip()}
            ws.send(json.dumps(message_json))

while True:
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    time.sleep(5)