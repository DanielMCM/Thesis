from influxdb import DataFrameClient
from API.Main.Client import Web_Client, Client
from Database.Functions.Helpers import *
from functools import partial
import time
import pandas as pd
from datetime import datetime

##TODO IF TIME --> CREATE CONFIG FILE WITH CLASS CONTAINING DFs TO MOVE PROCESS_MESSAGES TO OTHER FILES
##GLOBAL VARIABLES DO NOT WORK ACROSS MODULES/FILES

def nothing(msg):
    pass

def process_message(msg, exchange, pair):
    global df, client
    try:
        if exchange == "Binance":
            t = BinanceToTime(msg["T"])
            p = float(msg["p"])
            Q = float(msg["q"])
        elif exchange == "Bitfinex" and type(msg) == type([]):
            if  msg[1] != "tu":
                if len(msg)>2:
                    l = msg[2]
                else: 
                    l = msg[1]
                for element in l:
                    t = BinanceToTime(element[1])
                    p = float(element[3])
                    Q = float(element[2])
                    d = pd.DataFrame({ "t": [t], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                        "Price":[p],
                        "Q": Q
                    })
                    df = df.append(d)
                    tempdf = df
        elif exchange == "BitFlyer":
            if type(msg["params"]["message"][0]) == type([]):
                # This probably can be deleted
                for element in msg["params"]["message"][0]:
                    print("MULTITUD!")
                    t = datetime.strptime(element["exec_date"][:26],"%Y-%m-%dT%H:%M:%S.%f")
                    p = float(element["price"])
                    Q = float(element["size"])
                    d = pd.DataFrame({ "t": [t], 
                            "Host": [exchange], 
                            "Pair": [pair],
                            "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                            "Price":[p],
                            "Q":Q
                        })
                    df = df.append(d)
            else:
                t = datetime.strptime(msg["params"]["message"][0]["exec_date"][:26],"%Y-%m-%dT%H:%M:%S.%f")
                p = float(msg["params"]["message"][0]["price"])
                Q = float(msg["params"]["message"][0]["size"])
                d = pd.DataFrame({ "t": [t], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                        "Price":[p],
                        "Q":Q
                    })
                
                df = df.append(d)
            print("PASSED!!!")
        elif exchange == "Bithumb":
            if msg["code"] == "00006":
                for element in msg["data"]:
                    t = BinanceToTime(int(element["t"])*1000)
                    p = float(element["p"])
                    Q = float(element["v"])
                    d = pd.DataFrame({ "t": [t], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                        "Price":[p],
                        "Q": Q
                    })
                    df = df.append(d)
            else:
                t = BinanceToTime(int(msg["data"]["t"])*1000)
                p = float(msg["data"]["p"])
                Q = float(msg["data"]["v"])
        elif exchange == "Bitstamp":
            t = BinanceToTime(int(msg["data"]["timestamp"])*1000)
            p = float(msg["data"]["price"])
            Q = float(msg["data"]["amount"])
        elif exchange == "Coinbase":
            t = datetime.strptime(msg["time"],"%Y-%m-%dT%H:%M:%S.%fZ")
            #.strftime('%Y-%m-%d %H:%M:%S.%f')
            p = float(msg["price"])
            Q = float(msg["size"])
        elif exchange == "Huobi":
            t = BinanceToTime(msg["tick"]["data"][0]["ts"])
            p = float(msg["tick"]["data"][0]["price"])
            Q = float(msg["tick"]["data"][0]["amount"])
        elif exchange == "Kraken":
            t = BinanceToTime(int(round(float(msg[1][0][2])*1000)))
            p = float(msg[1][0][0])
            Q = float(msg[1][0][1])

        if exchange not in ["BitFlyer", "Bitfinex", "Bithumb"]:
            d = pd.DataFrame({ "t": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                    "Price":[p],
                    "Q": Q
                })
            df = df.append(d)
        #print(d)
    except Exception as e:
        if type(msg) == type([]):
            if msg[1] == "hb":
                pass
        else:
            if "type" in msg:
                if msg["type"] == "subscriptions":
                     pass
            elif "event" in msg:
                if msg["event"] in ["info", "subscribed",'bts:subscription_succeeded']:
                    pass
            elif "code" in msg:
                if  msg["code"] in ["00002", "00001", "0"]:
                    pass
            elif "method" in msg:
                if msg["method"] == "subscribe":
                    pass
            elif "ping" in msg:
                pass
            elif "id" in msg:
                if msg["id"] == "id1":
                    pass
            else:
                print(msg)
                print("MARKET - message type: Other - Market: " + exchange)

def process_message_2(msg, exchange, pair):
    global df_dif, df_Book
    try:
        add = 1
        if exchange == "Binance":
            t = BinanceToTime(msg["E"])
            S = [msg["u"],msg["U"]]
            bids = msg["b"]
            asks = msg["a"]
        elif exchange == "Bitfinex":
            if len(msg[1])>3:
                d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg[0]],
                        "Bids":[msg[1][0:99]], 
                        "Asks":[msg[1][100::]]
                    })
                df_Book = df_Book.append(d_2)
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = msg[1][0]
                bids = msg[1][1]
                asks = msg[1][2]
        elif exchange == "BitFlyer":
            if msg["params"]["channel"][0:25] == "lightning_board_snapshot_":
                d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["params"]["channel"]],
                        "Bids":[msg["params"]["message"]["bids"]], 
                        "Asks":[msg["params"]["message"]["asks"]]
                    })
                df_Book = df_Book.append(d_2)
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = "-"
                bids = msg["params"]["message"]["bids"]
                asks = msg["params"]["message"]["asks"]
        elif exchange == "Bithumb":
            if msg["code"] == "00006":
                d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["timestamp"]],
                        "Bids":[msg["data"]["b"]], 
                        "Asks":[msg["data"]["s"]]
                    })
                df_Book = df_Book.append(d_2)
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = [msg["data"]["ver"], msg["timestamp"]]
                bids = msg["data"]["b"]
                asks = msg["data"]["s"]
        elif exchange == "Bitstamp":
            if len(msg["data"]["bids"]) == 100:
                d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["data"]["microtimestamp"]],
                        "Bids":[msg["data"]["bids"]], 
                        "Asks":[msg["data"]["asks"]]
                    })
                df_Book = df_Book.append(d_2)
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = [msg["data"]["microtimestamp"]]
                bids = msg["data"]["bids"]
                asks = msg["data"]["asks"]
        elif exchange == "Coinbase":
            if msg["type"] == "snapshot":
                d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg["bids"]], 
                        "Asks":[msg["asks"]]
                    })
                df_Book = df_Book.append(d_2)
                add = 0
            elif msg["type"] == "match":
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = [msg["time"]]
                bids = [msg["side"],msg["size"],msg["price"],msg["sequence"]]
                asks = "match"
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = [msg["time"]]
                bids = msg["changes"]
                asks = "-"

        elif exchange == "Huobi":
            d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "LastUpdateID": [msg["tick"]["ts"]],
                    "Bids":[msg["tick"]["bids"]], 
                    "Asks":[msg["tick"]["asks"]]
                })
            df_Book = df_Book.append(d_2)
            add = 0
        elif exchange == "Kraken":
            if "as" in msg[1]:
                d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg[1]["bs"]], 
                        "Asks":[msg[1]["as"]]
                    })
                df_Book = df_Book.append(d_2)
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = ["-"]
                if "b" in msg[1]:
                    bids = msg[1]["b"]
                else:
                    bids = "-"
                if "a" in msg[1]:
                    asks = msg[1]["a"]
                else:
                    asks = "-"
        if add == 1:
            d = pd.DataFrame({ "t_recorded": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Seg": [S],
                    "Bids":[bids],
                    "Asks":[asks]
                })
            df_dif = df_dif.append(d)
    except Exception as e:
        if type(msg) == type([]):
            if msg[1] == "hb":
                pass
        else:
            if "type" in msg:
                if msg["type"] == "subscriptions":
                     pass
            elif "event" in msg:
                if msg["event"] in ["info", "subscribed",'bts:subscription_succeeded']:
                    pass
            elif "code" in msg:
                if  msg["code"] in ["00002", "00001"]:
                    pass
            elif "method" in msg:
                if msg["method"] == "subscribe":
                    pass
            elif "ping" in msg:
                pass
            elif "id" in msg:
                if msg["id"] == "id1":
                    pass
            else:
                print(msg)
                print("MARKET - message type: Other - Market: " + exchange)

def loadPair(Pair_1, Pair_2, time_wait, dbname, type):
    global client, df_Book, df_dif, df

    #if type == "Ticker":
    #    global df
    #    df = pd.DataFrame(columns = ["t", "Host", "Pair", "Event_Time", "Write_Time", "Price"])
    #elif type == "Book":
    #    global df_Book, df_dif
    #    df_Book = pd.DataFrame(columns = ["t_recorded","Host","Pair","LastUpdateID","Bids","Asks"])
    #    df_dif = pd.DataFrame(columns = ["t_recorded", "Host", "Pair", "Seg", "Bids", "Asks"])
    #else:
    df_Book = pd.DataFrame(columns = ["t_recorded","Host","Pair","LastUpdateID","Bids","Asks"])
    df_dif = pd.DataFrame(columns = ["t_recorded", "Host", "Pair", "Seg", "Bids", "Asks"])
    df = pd.DataFrame(columns = ["t", "Host", "Pair", "Event_Time", "Write_Time", "Price", "Q"])

    client = DataFrameClient('localhost', 8086, 'root', 'root')

    #if dbname not in client.get_list_database():
    #    client.create_database(dbname)
    #else:
    #    client.drop_database(dbname)
    #    client.create_database(dbname)

    if {"name":"Markets"} not in client.get_list_database():
        client.create_database("Markets")
    else:
        client.drop_database("Markets")
        client.create_database("Markets")
    #if "difBook" not in client.get_list_database():
    #    client.create_database("difBook")
    #else:
    #    client.drop_database("difBook")
    #    client.create_database("difBook")

    Binance = Web_Client.Binance()
    Binance_REST = Client.Binance()

    Bitfinex = Web_Client.Bitfinex()

    BitFlyer = Web_Client.BitFlyer()
    BitFlyer2 = Web_Client.BitFlyer()

    Bithumb = Web_Client.Bithumb()
    Bitstamp = Web_Client.Bitstamp()
    Bitstamp2 = Web_Client.Bitstamp()

    Coinbase = Web_Client.Coinbase()
    Huobi = Web_Client.Huobi()
    Kraken = Web_Client.Kraken()

    if type == "Ticker" or type == "all":
        Binance.start_trade_socket('ethbtc', partial(process_message,exchange = "Binance", pair = "ethbtc"))
        Bitfinex.start_trades("tETHBTC", partial(process_message,exchange = "Bitfinex", pair = "ethbtc"))
        BitFlyer.start_executions("ETH-BTC", partial(process_message,exchange = "BitFlyer", pair = "ethbtc"))
        Bithumb.start_trade('ETH-BTC', partial(process_message,exchange = "Bithumb", pair = "ethbtc"))
        Bitstamp.start_ticker('ethbtc', partial(process_message,exchange = "Bitstamp", pair = "ethbtc"))
        Coinbase.start_matches('ETH-BTC', partial(process_message,exchange = "Coinbase", pair = "ethbtc"))
        Huobi.start_trade('ethbtc', partial(process_message,exchange = "Huobi", pair = "ethbtc"))
        Kraken.start_trade('ETH/XBT', partial(process_message,exchange = "Kraken", pair = "ethbtc"))
    if type == "Book" or type == "all":
        Binance.start_depth_socket("ethbtc",partial(process_message_2,exchange = "Binance", pair = "ethbtc"))
        Bitfinex.start_raw_book("tETHBTC", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc"))
        BitFlyer.start_book("ETH_BTC", partial(process_message_2,exchange = "BitFlyer", pair = "ethbtc"))
        BitFlyer2.start_book_updates("ETH_BTC", partial(process_message_2,exchange = "BitFlyer", pair = "ethbtc"))
        Bithumb.start_order_book('ETH-BTC', partial(process_message_2,exchange = "Bithumb", pair = "ethbtc"))
        Bitstamp2.start_liveFull('ethbtc', partial(process_message_2,exchange = "Bitstamp", pair = "ethbtc"))
        Bitstamp.start_orderBook('ethbtc', partial(process_message_2,exchange = "Bitstamp", pair = "ethbtc"))
        Coinbase.start_ticker('ETH-BTC', partial(process_message_2,exchange = "Coinbase", pair = "ethbtc"))
        Coinbase.start_heartbeat('ETH-BTC', nothing)
        Huobi.start_depth('ethbtc', partial(process_message_2,exchange = "Huobi", pair = "ethbtc"))
        Kraken.start_book('ETH/XBT', partial(process_message_2,exchange = "Kraken", pair = "ethbtc"))

    Binance.start()
    Bitfinex.start()
    BitFlyer.start()
    BitFlyer2.start()
    Bithumb.start()
    Bitstamp.start()
    Bitstamp2.start()
    Coinbase.start()
    Huobi.start()
    Kraken.start()
    static_order = []
    if type == "Ticker":
        time.sleep(10)
    else:
        for i in range(6):
            _msg_ = Binance_REST.depth("ETHBTC", limit = 1000)
            d_2 = pd.DataFrame({ "t_recorded": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": ["Binance"], 
                        "Pair": ["ethbtc"],
                        "LastUpdateID": [_msg_["lastUpdateId"]],
                        "Bids":[_msg_["bids"]], 
                        "Asks":[_msg_["asks"]]
                    })
            df_Book = df_Book.append(d_2)
            time.sleep(10)
        BitFlyer.close()
        Bitstamp.close()
    time.sleep(240)
    for i in range(11):
        df_temp = df
        df_dif_temp = df_dif
        df_Book_temp = df_Book

        df_temp = df_temp.set_index("t")
        df_dif_temp = df_dif_temp.set_index("t_recorded")
        df_Book_temp = df_Book_temp.set_index("t_recorded")

        print("WRITING " + str(len(df_temp)) + " lines in Markets : " + str(BinanceToTime(int(round(time.time() * 1000)))))
        client.write_points(df_temp,"Price", time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'])
        df = df.iloc[len(df_temp):]

        print("WRITING " + str(len(df_dif_temp)) + " lines in difBook : " + str(BinanceToTime(int(round(time.time() * 1000)))))
        client.write_points(df_dif_temp,"difBook",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'])
        df_dif = df_dif.iloc[len(df_dif_temp):]

        print("WRITING " + str(len(df_Book_temp)) + " lines in Book : " + str(BinanceToTime(int(round(time.time() * 1000)))))
        client.write_points(df_Book_temp,"Book",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'])
        df_Book = df_Book.iloc[len(df_Book_temp):]

        time.sleep(300)

    Bitfinex.close()
    BitFlyer2.close()
    Bithumb.close()
    Bitstamp2.close()
    Coinbase.close()
    Huobi.close()
    Kraken.close()

    df_temp = df.set_index("t")
    df_dif_temp = df_dif.set_index("t_recorded")
    df_Book_temp = df_Book.set_index("t_recorded")
    
    client.write_points(df_temp,"Price", time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'])
    client.write_points(df_dif_temp,"difBook",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'])
    client.write_points(df_Book_temp,"Book",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'])

    print("..END..")

#loadPair("ETH", "BTC", 50, "Market", "Ticker")

