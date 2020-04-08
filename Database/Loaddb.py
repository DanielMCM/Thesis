from influxdb import InfluxDBClient
from API.Main.Client import Web_Client
from functools import partial
import time
import pandas as pd
from datetime import datetime

def BinanceToTime(T):
    return datetime.fromtimestamp(T/1000).strftime('%Y-%m-%d %H:%M:%S.%f')

#def timestampToTime(T):
#    print(T)
#    return datetime.fromtimestamp(T).strftime('%Y-%m-%d %H:%M:%S.%f')

def process_message(msg, exchange, pair):
    global df
    try:
        if exchange == "Binance":
            t = BinanceToTime(msg["T"])
            p = msg["p"]
        elif exchange == "Bitfinex" and type(msg) == type([]) and len(msg)>2:
            print("Al menos uno!")
            t = BinanceToTime(msg[2][1])
            p = msg[2][3]
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
    
    Binance.start_trade_socket('ethbtc', partial(process_message,exchange = "Binance", pair = "ethbtc"))
    Bitfinex.start_trades("tETHBTC", partial(process_message,exchange = "Bitfinex", pair = "ethbtc"))

    Binance.start()
    Bitfinex.start()
    time.sleep(120)
    print(df)

loadPair("ETH", "BTC", 50, "Market")
