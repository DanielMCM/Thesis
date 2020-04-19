from influxdb import InfluxDBClient
from API.Main.Client import Web_Client, Client
from Database.Functions.Helpers import *
from functools import partial
import time
import pandas as pd
from datetime import datetime

##TODO IF TIME --> CREATE CONFIG FILE WITH CLASS CONTAINING DFs TO MOVE PROCESS_MESSAGES TO OTHER FILES
##GLOBAL VARIABLES DO NOT WORK ACROSS MODULES/FILES

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
            for element in msg["params"]["message"][0]:
                t = datetime.strptime(msg["params"]["message"][0]["exec_date"][:26],"%Y-%m-%dT%H:%M:%S.%f")
                p = float(msg["params"]["message"][0]["price"])
                d = pd.DataFrame({ "t": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Write_Time": [BinanceToTime(int(round(time.time() * 1000)))],
                    "Price":[p]
                })
                df = df.append(d)  
        elif exchange == "Bithumb":
            t = timestampToTime(msg["data"]["t"])
            p = float(msg["data"]["p"])
        elif exchange == "Bitstamp":
            t = timestampToTime(msg["data"]["timestamp"])
            p = float(msg["data"]["price"])
        elif exchange == "Coinbase":
            t = datetime.strptime(msg["time"],"%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d %H:%M:%S.%f')
            p = float(msg["price"])
        elif exchange == "Huobi":
            t = BinanceToTime(msg["tick"]["data"][0]["ts"])
            p = float(msg["tick"]["data"][0]["price"])
        elif exchange == "Kraken":
            t = BinanceToTime(int(round(float(msg[1][0][2])*1000)))
            p = float(msg[1][0][0])
        if exchange != "BitFlyer":
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

def process_message_2(msg, exchange, pair):
    global df_dif
    try:
        if exchange == "Binance":
            t = BinanceToTime(msg["E"])
            u = msg["u"]
            U = msg["U"]
            bids = msg["b"]
            asks = msg["a"]
        elif exchange == "Bitfinex" and type(msg) == type([]) and len(msg)>2:
            pass
        elif exchange == "BitFlyer":
            pass
        elif exchange == "Bithumb":
            pass
        elif exchange == "Bitstamp":
            pass
        elif exchange == "Coinbase":
            pass
        elif exchange == "Huobi":
            pass
        elif exchange == "Kraken":
            pass
        if exchange != "BitFlyer":
            pass
        d = pd.DataFrame({ "t_recorded": [t], 
                "Host": [exchange], 
                "Pair": [pair],
                "Seg": [[u,U]],
                "Bids":[[bids]],
                "Asks":[[asks]]
            })
        df_dif = df_dif.append(d)
        print("YESYES!!")
        #print(d)
    except:
        print(df_dif)
        print(msg)
        print("message type: Other")

def loadPair(Pair_1, Pair_2, time_wait, dbname, type):
    global dummy
    dummy = 10
    if type == "Ticker":
        global df
        df = pd.DataFrame(columns = ["t", "Host", "Pair", "Event_Time", "Write_Time", "Price"])
    else:
        global df_Book, df_dif
        df_Book = pd.DataFrame(columns = ["t_recorded","Host","Pair","LastUpdateID","Bids","Asks"])
        df_dif = pd.DataFrame(columns = ["t_recorded", "Host", "Pair", "Seg", "Bids", "Asks"])

    client = InfluxDBClient('localhost', 8086, 'root', 'root')

    if dbname not in client.get_list_database():
        client.create_database(dbname)

    #client = InfluxDBClient('localhost', 8086, 'root', 'root', dbname)

    Binance = Web_Client.Binance()
    Binance_REST = Client.Binance()

    Bitfinex = Web_Client.Bitfinex()

    BitFlyer = Web_Client.BitFlyer()

    Bithumb = Web_Client.Bithumb()
    Bitstamp = Web_Client.Bitstamp()
    Coinbase = Web_Client.Coinbase()
    Huobi = Web_Client.Huobi()
    Kraken = Web_Client.Kraken()

    if type == "Ticker":
        Binance.start_trade_socket('ethbtc', partial(process_message,exchange = "Binance", pair = "ethbtc"))
        Bitfinex.start_trades("tETHBTC", partial(process_message,exchange = "Bitfinex", pair = "ethbtc"))
        BitFlyer.start_executions("BTC_JPY", partial(process_message,exchange = "BitFlyer", pair = "ethbtc"))
        Bithumb.trade('ETH-BTC', partial(process_message,exchange = "Bithumb", pair = "ethbtc"))
        Bitstamp.start_ticker('ethbtc', partial(process_message,exchange = "Bitstamp", pair = "ethbtc"))
        Coinbase.start_matches('ETH-BTC', partial(process_message,exchange = "Coinbase", pair = "ethbtc"))
        Huobi.start_trade('ethbtc', partial(process_message,exchange = "Huobi", pair = "ethbtc"))
        Kraken.start_trade('ETH/XBT', partial(process_message,exchange = "Kraken", pair = "ethbtc"))
    elif type == "Book":
        Binance.start_depth_socket("ethbtc",partial(process_message_2,exchange = "Binance", pair = "ethbtc"))

    Binance.start()
    Bitfinex.start()
    BitFlyer.start()
    Bithumb.start()
    Bitstamp.start()
    Coinbase.start()
    Huobi.start()
    Kraken.start()
    static_order = []

    if type == "Ticker":
        time.sleep(10)
        print(df)
    else:
        for i in range(3):
            print("HELLO!!")
            _msg_ = Binance_REST.depth("ETHBTC", limit = 1000)
            d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": ["Binance"], 
                        "Pair": ["ethbtc"],
                        "LastUpdateID": [_msg_["lastUpdateId"]],
                        "Bids":[_msg_["bids"]], 
                        "Asks":[_msg_["asks"]]
                    })
            df_Book = df_Book.append(d_2)
            time.sleep(5)
        print(df_Book)
        print(df_dif)

#loadPair("ETH", "BTC", 50, "Market", "Ticker")

