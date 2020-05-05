from influxdb import DataFrameClient
from API.Main.Client import Web_Client, Client
from Database.Functions.Helpers import *
from functools import partial
import time
import pandas as pd
from datetime import datetime, timedelta

##TODO IF TIME --> CREATE CONFIG FILE WITH CLASS CONTAINING DFs TO MOVE PROCESS_MESSAGES TO OTHER FILES
##GLOBAL VARIABLES DO NOT WORK ACROSS MODULES/FILES

def nothing(msg):
    pass

def process_message(msg, exchange, pair):
    try:
        add = 1
        if exchange == "Binance":
            t = BinanceToTime(msg["T"])
            p = float(msg["p"])
            Q = float(msg["q"])
        elif exchange == "Bitfinex" and type(msg) == type([]):
            if  msg[1] != "tu":
                if len(msg)>2 and type(msg[2][0]) == type([]):
                    l = msg[2]
                elif len(msg)>2:
                    l = [msg[2]]
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
                    exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})                   
        elif exchange == "BitFlyer":
            if type(msg["params"]["message"][0]) == type([]):
                # This probably can be deleted
                for element in msg["params"]["message"][0]:
                    t = datetime.strptime(element["exec_date"][:26],"%Y-%m-%dT%H:%M:%S.%f") + timedelta(hours = 2)
                    p = float(element["price"])
                    Q = float(element["size"])
                    d = pd.DataFrame({ "t": [t], 
                            "Host": [exchange], 
                            "Pair": [pair],
                            "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                            "Price":[p],
                            "Q":Q
                        })
                    exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})
            else:
                t = datetime.strptime(msg["params"]["message"][0]["exec_date"][:26],"%Y-%m-%dT%H:%M:%S.%f") + timedelta(hours = 2)
                p = float(msg["params"]["message"][0]["price"])
                Q = float(msg["params"]["message"][0]["size"])
                d = pd.DataFrame({ "t": [t], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                        "Price":[p],
                        "Q":Q
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})             
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
                    exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})
            else:
                t = BinanceToTime(int(msg["data"]["t"])*1000)
                p = float(msg["data"]["p"])
                Q = float(msg["data"]["v"])
        elif exchange == "Bitstamp":
            t = BinanceToTime(int(msg["data"]["timestamp"])*1000)
            p = float(msg["data"]["price"])
            Q = float(msg["data"]["amount"])
        elif exchange == "Coinbase":
            t = datetime.strptime(msg["time"],"%Y-%m-%dT%H:%M:%S.%fZ")  + timedelta(hours = 2)
            #.strftime('%Y-%m-%d %H:%M:%S.%f')
            p = float(msg["price"])
            Q = float(msg["size"])
        elif exchange == "Huobi":
            t = BinanceToTime(msg["tick"]["data"][0]["ts"])
            p = float(msg["tick"]["data"][0]["price"])
            Q = float(msg["tick"]["data"][0]["amount"])
        elif exchange == "Kraken":
            t = datetime.fromtimestamp(int(round(float((msg[1][0][2]))))).strftime('%Y-%m-%d %H:%M:%S') + "." +  msg[1][0][2][11::]
            p = float(msg[1][0][0])
            Q = float(msg[1][0][1])

        if exchange not in ["BitFlyer", "Bitfinex"] and add == 1:
            d = pd.DataFrame({ "t": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Write_Time": [BinanceToTime_string(int(round(time.time() * 1000)))],
                    "Price":[p],
                    "Q": Q
                })
            exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})

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
    try:
        add = 1
        if exchange == "Binance":
            t = BinanceToTime(msg["E"])
            S = [msg["u"],msg["U"]]
            bids = msg["b"]
            asks = msg["a"]
        elif exchange == "Bitfinex":
            if len(msg[1])>3:
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg[0]],
                        "Bids":[msg[1][0:99]], 
                        "Asks":[msg[1][100::]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = msg[1][0]
                bids = str(msg[1][1])
                asks = str(msg[1][2])
        elif exchange == "BitFlyer":
            if msg["params"]["channel"][0:25] == "lightning_board_snapshot_":
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["params"]["channel"]],
                        "Bids":[msg["params"]["message"]["bids"]], 
                        "Asks":[msg["params"]["message"]["asks"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = "-"
                bids = msg["params"]["message"]["bids"]
                asks = msg["params"]["message"]["asks"]
        elif exchange == "Bithumb":
            if msg["code"] == "00006":
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["timestamp"]],
                        "Bids":[msg["data"]["b"]], 
                        "Asks":[msg["data"]["s"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = [msg["data"]["ver"], msg["timestamp"]]
                bids = msg["data"]["b"]
                asks = msg["data"]["s"]
        elif exchange == "Bitstamp":
            if len(msg["data"]["bids"]) == 100:
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["data"]["microtimestamp"]],
                        "Bids":[msg["data"]["bids"]], 
                        "Asks":[msg["data"]["asks"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = BinanceToTime(int(round(time.time() * 1000)))
                S = [msg["data"]["microtimestamp"]]
                bids = msg["data"]["bids"]
                asks = msg["data"]["asks"]
        elif exchange == "Coinbase":
            if msg["type"] == "snapshot":
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg["bids"]], 
                        "Asks":[msg["asks"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
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
            d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "LastUpdateID": [msg["tick"]["ts"]],
                    "Bids":[msg["tick"]["bids"]], 
                    "Asks":[msg["tick"]["asks"]]
                })
            exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
            add = 0
        elif exchange == "Kraken":
            if "as" in msg[1]:
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg[1]["bs"]], 
                        "Asks":[msg[1]["as"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
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
            d = pd.DataFrame({ "t": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Seg": [S],
                    "Bids":[bids],
                    "Asks":[asks]
                })
            exec("globals()['" + exchange + "_" + pair.upper() + "_df_dif'] = globals()['" + exchange + "_" + pair.upper() + "_df_dif'].append(d)", globals(),{"d":d})
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

def loadPair():
    groups = ["Binance_ETHBTC", "Bitfinex_ETHBTC", "BitFlyer_ETHBTC", "Bitstamp_ETHBTC", 
            "Bithumb_ETHBTC", "Coinbase_ETHBTC", "Huobi_ETHBTC", "Kraken_ETHBTC", 
            "Bitfinex_BTCUSD", "Bitstamp_BTCUSD", "BitFlyer_BTCUSD", "Coinbase_BTCUSD",
            "Bitfinex_ETHUSD", "Bitstamp_ETHUSD", "Coinbase_ETHUSD",
            "Bitfinex_XTZUSD", "Coinbase_XTZUSD",
            "Coinbase_DASHUSD"]

    text = "global client "
    for element in groups:
        text = text + "," + element + "_df_dif"
        text = text + "," + element + "_df"
        text = text + "," + element + "_df_Book"
    exec(text, globals())
    for element in groups:

        exec(element + "_df_dif = pd.DataFrame(columns = ['t', 'Host', 'Pair', 'Seg', 'Bids', 'Asks'])", globals())
        exec(element + "_df = pd.DataFrame(columns = ['t', 'Host', 'Pair', 'Write_Time', 'Price', 'Q'])", globals())
        exec(element + "_df_Book = pd.DataFrame(columns = ['t', 'Host', 'Pair', 'LastUpdateID','Bids','Asks'])" , globals())


    client = DataFrameClient('localhost', 8086, 'root', 'root')

    #if {"name":"Markets"} not in client.get_list_database():
    #    client.create_database("Markets")
    #else:
    #    client.drop_database("Markets")
    #    client.create_database("Markets")
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

    Binance.start_trade_socket('ethbtc', partial(process_message,exchange = "Binance", pair = "ethbtc"))
    Bitfinex.start_trades("tETHBTC", partial(process_message,exchange = "Bitfinex", pair = "ethbtc"))        
    BitFlyer.start_executions("ETH_BTC", partial(process_message,exchange = "BitFlyer", pair = "ethbtc"))
    Bithumb.start_trade('ETH-BTC', partial(process_message,exchange = "Bithumb", pair = "ethbtc"))
    Bitstamp.start_ticker('ethbtc', partial(process_message,exchange = "Bitstamp", pair = "ethbtc"))
    Coinbase.start_matches('ETH-BTC', partial(process_message,exchange = "Coinbase", pair = "ethbtc"))
    Huobi.start_trade('ethbtc', partial(process_message,exchange = "Huobi", pair = "ethbtc"))
    Kraken.start_trade('ETH/XBT', partial(process_message,exchange = "Kraken", pair = "ethbtc"))

    time.sleep(1)

    Bitfinex.start_trades("tBTCUSD", partial(process_message,exchange = "Bitfinex", pair = "btcusd"))
    Bitstamp.start_ticker('btcusd', partial(process_message,exchange = "Bitstamp", pair = "btcusd"))
    BitFlyer.start_executions("BTC_USD", partial(process_message,exchange = "BitFlyer", pair = "btcusd"))
    Coinbase.start_matches('BTC-USD', partial(process_message,exchange = "Coinbase", pair = "btcusd"))

    time.sleep(1)

    Bitfinex.start_trades("tETHUSD", partial(process_message,exchange = "Bitfinex", pair = "ethusd"))
    Bitstamp.start_ticker('ethusd', partial(process_message,exchange = "Bitstamp", pair = "ethusd"))
    Coinbase.start_matches('ETH-USD', partial(process_message,exchange = "Coinbase", pair = "ethusd"))

    time.sleep(1)

    Bitfinex.start_trades("tXTZUSD", partial(process_message,exchange = "Bitfinex", pair = "xtzusd"))
    Coinbase.start_matches('XTZ-USD', partial(process_message,exchange = "Coinbase", pair = "xtzusd"))

    Coinbase.start_matches('DASH-USD', partial(process_message,exchange = "Coinbase", pair = "dashusd"))

    ##################################################
    ##################################################
    ##################################################
    ##################################################
    
    #BitFlyer.start_ticker("ETH_BTC", partial(process_message_2,exchange = "BitFlyer", pair = "ethbtc")) <------------ TODO
    #Bitfinex.start_candles("tETHBTC", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc"))
    #Bitfinex.start_candles("tBTCUSD", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc"))
    #Bitfinex.start_candles("tETHUSD", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc"))
    #Bitfinex.start_candles("tXTZUSD", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc"))

    Binance.start_depth_socket("ethbtc",partial(process_message_2,exchange = "Binance", pair = "ethbtc"))
    Bitfinex.start_raw_book("tETHBTC", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc"))
    BitFlyer.start_book("ETH_BTC", partial(process_message_2,exchange = "BitFlyer", pair = "ethbtc"))
    BitFlyer2.start_book_updates("ETH_BTC", partial(process_message_2,exchange = "BitFlyer", pair = "ethbtc"))
    Bithumb.start_order_book('ETH-BTC', partial(process_message_2,exchange = "Bithumb", pair = "ethbtc"))
    Bitstamp2.start_liveFull('ethbtc', partial(process_message_2,exchange = "Bitstamp", pair = "ethbtc"))
    Bitstamp.start_orderBook('ethbtc', partial(process_message_2,exchange = "Bitstamp", pair = "ethbtc"))
    Coinbase.start_ticker('ETH-BTC', partial(process_message_2,exchange = "Coinbase", pair = "ethbtc"))
    Huobi.start_depth('ethbtc', partial(process_message_2,exchange = "Huobi", pair = "ethbtc"))
    Kraken.start_book('ETH/XBT', partial(process_message_2,exchange = "Kraken", pair = "ethbtc"))

    time.sleep(1)

    Bitfinex.start_raw_book("tBTCUSD", partial(process_message_2,exchange = "Bitfinex", pair = "btcusd"))
    Bitstamp2.start_liveFull('btcusd', partial(process_message_2,exchange = "Bitstamp", pair = "btcusd"))
    Bitstamp.start_orderBook('btcusd', partial(process_message_2,exchange = "Bitstamp", pair = "btcusd"))
    BitFlyer.start_book("BTC_USD", partial(process_message_2,exchange = "BitFlyer", pair = "btcusd"))
    BitFlyer.start_ticker("BTC_USD", partial(process_message_2,exchange = "BitFlyer", pair = "ethbtc"))
    BitFlyer2.start_book_updates("BTC_USD", partial(process_message_2,exchange = "BitFlyer", pair = "btcusd"))
    Coinbase.start_ticker('BTC-USD', partial(process_message_2,exchange = "Coinbase", pair = "btcusd"))

    time.sleep(1)

    Bitfinex.start_raw_book("tETHUSD", partial(process_message_2,exchange = "Bitfinex", pair = "ethusd"))
    Bitstamp2.start_liveFull('ethusd', partial(process_message_2,exchange = "Bitstamp", pair = "ethusd"))
    Bitstamp.start_orderBook('ethusd', partial(process_message_2,exchange = "Bitstamp", pair = "ethusd"))
    Coinbase.start_ticker('ETH-USD', partial(process_message_2,exchange = "Coinbase", pair = "ethusd"))

    time.sleep(1)

    Bitfinex.start_raw_book("tXTZUSD", partial(process_message_2,exchange = "Bitfinex", pair = "xtzusd"))
    Coinbase.start_ticker('XTZ-USD', partial(process_message_2,exchange = "Coinbase", pair = "xtzusd"))

    time.sleep(1)

    Coinbase.start_ticker('DASH-USD', partial(process_message_2,exchange = "Coinbase", pair = "dashusd"))
        
    time.sleep(1)

    Coinbase.start_heartbeat('ETH-BTC', nothing)


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

    for i in range(6):
        _msg_ = Binance_REST.depth("ETHBTC", limit = 1000)
        d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
                    "Host": ["Binance"], 
                    "Pair": ["ethbtc"],
                    "LastUpdateID": [_msg_["lastUpdateId"]],
                    "Bids":[_msg_["bids"]], 
                    "Asks":[_msg_["asks"]]
                })
        var = globals()
        var["d_2"] = d_2
        exec("Binance_ETHBTC_df_Book = Binance_ETHBTC_df_Book.append(d_2)", var)
        time.sleep(10)
    BitFlyer.close()
    Bitstamp.close()
    time.sleep(90)
    for i in range(11):
        print("CHECKING DATAFRAMES: " +  str(BinanceToTime(int(round(time.time() * 1000)))))
        for element in groups:
            ldict = {}
            var = globals()
            exec("df_temp = " + element + "_df",var, ldict)
            df_temp = ldict["df_temp"]
            exec("df_Book_temp = " + element + "_df_Book",var, ldict)
            df_Book_temp = ldict["df_Book_temp"]
            exec("df_dif_temp = " + element + "_df_dif",var, ldict)
            df_dif_temp = ldict["df_dif_temp"]
            
            var.update(ldict)
            
            if len(df_Book_temp)>2500:
                print("-------------------------------")
                df_Book_temp = df_Book_temp.set_index("t")
                print("WRITING " + str(len(df_Book_temp)) + " lines in Book FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
                client.write_points(df_Book_temp,"Book",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'], batch_size = 1000)
                exec(element + "_df_Book = " + element + "_df_Book.iloc[len(df_Book_temp):]", var)
                print("-------------------------------")

            if len(df_dif_temp)>2500:
                print("-------------------------------")
                df_dif_temp = df_dif_temp.set_index("t")
                print("WRITING " + str(len(df_dif_temp)) + " lines in difBook FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
                client.write_points(df_dif_temp,"difBook",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'], batch_size = 1000)
                var = globals()
                var["d_2"] = d_2
                exec(element +  "_df_dif = " + element + "_df_dif.iloc[len(df_dif_temp):]", var)
                print("-------------------------------")

            if len(df_temp)>2500:
                print("-------------------------------")
                df_temp = df_temp.set_index("t")
                print("WRITING " + str(len(df_temp)) + " lines in Markets FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
                client.write_points(df_temp,"Price", time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'], batch_size = 1000)
                exec(element +  "_df = " + element + "_df.iloc[len(df_temp):]", var)
                print("-------------------------------")
                
        #_msg_ = Binance_REST.depth("ETHBTC", limit = 1000)
        #d_2 = pd.DataFrame({ "t": [BinanceToTime(int(round(time.time() * 1000)))], 
        #                     "Host": ["Binance"], 
        #                     "Pair": ["ethbtc"],
        #                     "LastUpdateID": [_msg_["lastUpdateId"]],
        #                     "Bids":[_msg_["bids"]], 
        #                     "Asks":[_msg_["asks"]]
        #                    })
        #var = globals()
        #var["d_2"] = d_2
        #exec("Binance_ETHBTC_df_Book = Binance_ETHBTC_df_Book.append(d_2)", var)
        time.sleep(150)
    
    Binance.close()
    Bitfinex.close()
    BitFlyer2.close()
    Bithumb.close()
    Bitstamp2.close()
    Coinbase.close()
    Huobi.close()
    Kraken.close()

    for element in groups:
        ldict = {}
        var = globals()
        exec("df_temp = " + element + "_df",var, ldict)
        df_temp = ldict["df_temp"]
        exec("df_Book_temp = " + element + "_df_Book",var, ldict)
        df_Book_temp = ldict["df_Book_temp"]
        exec("df_dif_temp = " + element + "_df_dif",var, ldict)
        df_dif_temp = ldict["df_dif_temp"]
            
        var.update(ldict)

        print("-------------------------------")
        df_Book_temp = df_Book_temp.set_index("t")
        print("WRITING " + str(len(df_Book_temp)) + " lines in Book FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
        client.write_points(df_Book_temp,"Book",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'], batch_size = 1000)


        df_dif_temp = df_dif_temp.set_index("t")
        print("WRITING " + str(len(df_dif_temp)) + " lines in difBook FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
        client.write_points(df_dif_temp,"difBook",time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'], batch_size = 1000)
        var = globals()
        var["d_2"] = d_2


        df_temp = df_temp.set_index("t")
        print("WRITING " + str(len(df_temp)) + " lines in Markets FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
        client.write_points(df_temp,"Price", time_precision = "n", database = "Markets", tag_columns = ['Host', 'Pair'], batch_size = 1000)
        print("-------------------------------")

    print("..END..")

