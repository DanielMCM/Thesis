from influxdb import InfluxDBClient
from API.Main.Client import Web_Client
from functools import partial
import time
import pandas as pd
from datetime import datetime

def BinanceToTime(T):
    return datetime.fromtimestamp(T/1000).strftime('%Y-%m-%d %H:%M:%S.%f')

def timestampToTime(T): 
    T = int(T)
    return datetime.fromtimestamp(T).strftime('%Y-%m-%d %H:%M:%S.%f')

def microtimestampToTime(T): 
    T = int(T)
    return datetime.fromtimestamp(T/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')

def process_message(msg, exchange, pair):
    global df
    try:
        if exchange == "Binance":
            t = BinanceToTime(msg["T"])
            p = float(msg["p"])
        elif exchange == "Bitfinex" and type(msg) == type([]) and len(msg)>2:
            t = BinanceToTime(msg[2][1])
            p = float(msg[2][3])
        elif exchange == "BitFlyer":
            t = msg["params"]["message"][0]["exec_date"]
            p = float(msg["params"]["message"][0]["price"])
        elif exchange == "Bithumb":
            t = timestampToTime(msg["data"]["t"])
            p = float(msg["data"]["p"])
        elif exchange == "Bitstamp":
            t = timestampToTime(msg["data"]["timestamp"])
            p = float(msg["data"]["price"])
        elif exchange == "Coinbase":
            t = msg["time"]
            p = float(msg["price"])
        elif exchange == "Huobi":
            t = BinanceToTime(msg["tick"]["data"][0]["ts"])
            p = float(msg["tick"]["data"][0]["price"])
        elif exchange == "Kraken":
            t = BinanceToTime(int(round(float(msg[1][0][2])*1000)))
            p = float(msg[1][0][0])
        d = pd.DataFrame({ "t": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Write_Time": [BinanceToTime(int(round(time.time() * 1000)))],
                    "Price":[p]
                })
        df = df.append(d)  
        #print(d)
    except:
        print("message type: Other")
        
        print(msg)
        #print(msg[1][0][2])
        print(BinanceToTime(int(round(time.time() * 1000))))

def loadPair(Pair_1, Pair_2, time_wait, dbname):
    global df
    df = pd.DataFrame(columns = ["t", "Host", "Pair", "Event_Time", "Write_Time", "Price"])

    client = InfluxDBClient('localhost', 8086, 'root', 'root')

    if dbname not in client.get_list_database():
        client.create_database(dbname)

    client = InfluxDBClient('localhost', 8086, 'root', 'root', dbname)
    Binance = Web_Client.Binance()
    Bitfinex = Web_Client.Bitfinex()
    BitFlyer = Web_Client.BitFlyer()
    Bithumb = Web_Client.Bithumb()
    Bitstamp = Web_Client.Bitstamp()
    Coinbase = Web_Client.Coinbase()
    Huobi = Web_Client.Huobi()
    Kraken = Web_Client.Kraken()

    #Binance.start_trade_socket('ethbtc', partial(process_message,exchange = "Binance", pair = "ethbtc"))
    #Bitfinex.start_trades("tETHBTC", partial(process_message,exchange = "Bitfinex", pair = "ethbtc"))
    #BitFlyer.start_executions("ETH_BTC", partial(process_message,exchange = "BitFlyer", pair = "ethbtc"))
    #Bithumb.trade('ETH-BTC', partial(process_message,exchange = "Bithumb", pair = "ethbtc"))
    #Bitstamp.start_ticker('ethbtc', partial(process_message,exchange = "Bitstamp", pair = "ethbtc"))
    #Coinbase.start_matches('ETH-BTC', partial(process_message,exchange = "Coinbase", pair = "ethbtc"))
    #Huobi.start_trade('btcusdt', partial(process_message,exchange = "Huobi", pair = "ethbtc"))
    Kraken.start_trade('ETH/XBT', partial(process_message,exchange = "Kraken", pair = "ethbtc"))

    #Binance.start()
    #Bitfinex.start()
    #BitFlyer.start()
    #Bithumb.start()
    #Bitstamp.start()
    #Coinbase.start()
    #Huobi.start()
    Kraken.start()
    time.sleep(30)
    print(df)

loadPair("ETH", "BTC", 50, "Market")
