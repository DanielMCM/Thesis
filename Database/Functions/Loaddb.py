from influxdb import InfluxDBClient
from API.Main.Client import Web_Client, Client
from Database.Functions.Helpers import *
from functools import partial
import time
import pandas as pd
from datetime import datetime


def loadPair(Pair_1, Pair_2, time_wait, dbname, type):

    if type == "Ticker":
        global df
        df = pd.DataFrame(columns = ["t", "Host", "Pair", "Event_Time", "Write_Time", "Price"])
    else:
        global df_Book, df_dif
        df_Bin = pd.DataFrame(columns = ["t_recorded", "Host", "Pair", "Follow_up_utils", "Bids", "Asks"])

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
        df_Bin = df_Bin.append(d_2)
        time.sleep(10)
    print(df_Bin)

    #print(df)

#loadPair("ETH", "BTC", 50, "Market", "Ticker")

