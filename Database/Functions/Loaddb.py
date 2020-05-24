from influxdb import DataFrameClient
from API.Main.Client import Web_Client, Client
from Database.Functions.Helpers import *
from functools import partial
import time
import pandas as pd
from datetime import datetime, timedelta
import os

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
                        "Price":[p],
                        "Q": Q
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
                        "Price":[p],
                        "Q": Q
                    })        
                    exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})
            else:
                t = BinanceToTime(int(msg["data"]["t"])*1000)
                p = float(msg["data"]["p"])
                Q = float(msg["data"]["v"])
        elif exchange == "Bitstamp":
            t = pd.to_datetime(int(msg["data"]["microtimestamp"]), unit='us') + timedelta(hours = 2)
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
            t = datetime.fromtimestamp(float(msg[1][0][2]))
            p = float(msg[1][0][0])
            Q = float(msg[1][0][1])
        if exchange != "Bitfinex" and add == 1:
            d = pd.DataFrame({ "t": [t], 
                    "Host": [exchange], 
                    "Pair": [pair],
                    "Price":[p],
                    "Q": Q
                })
            exec("globals()['" + exchange + "_" + pair.upper() + "_df'] = globals()['" + exchange + "_" + pair.upper() + "_df'].append(d)", globals(),{"d":d})
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
                print(msg, file = globals()["file_out"])
                print(msg)
                print("MARKET - message type: Other - Market: " + exchange, file = globals()["file_out"])
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
                t_temp = BinanceToTime(msg[2])
                d_2 = pd.DataFrame({ "t": [t_temp], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg[1][0:99]], 
                        "Asks":[msg[1][100::]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                
                add = 0
            else:
                #We are going to try to map bitfinex with an order book requests
                t = BinanceToTime(msg[2])
                S = msg[1][1]
                bids = str(msg[1][0])
                asks = str(msg[1][2])
        elif exchange == "Bithumb":
            if msg["code"] == "00006":
                d_2 = pd.DataFrame({ "t": [BinanceToTime(int(msg["timestamp"]))], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": [msg["data"]["ver"]],
                        "Bids":[msg["data"]["b"]], 
                        "Asks":[msg["data"]["s"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = BinanceToTime(int(msg["timestamp"]))
                S = [msg["data"]["ver"], msg["timestamp"]]
                bids = msg["data"]["b"]
                asks = msg["data"]["s"]
        elif exchange == "Bitstamp":
            if len(msg["data"]["bids"]) == 100:
                d_2 = pd.DataFrame({ "t": [pd.to_datetime(int(msg["data"]["microtimestamp"]), unit='us') + timedelta(hours = 2)], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg["data"]["bids"]], 
                        "Asks":[msg["data"]["asks"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = pd.to_datetime(int(msg["data"]["microtimestamp"]), unit='us') + timedelta(hours = 2)
                S = [msg["data"]["microtimestamp"]]
                bids = msg["data"]["bids"]
                asks = msg["data"]["asks"]
        elif exchange == "Coinbase":
            if msg["type"] == "snapshot":
                t_temp = BinanceToTime(int(round(time.time() * 1000)))
                d_2 = pd.DataFrame({ "t": [t_temp], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg["bids"]], 
                        "Asks":[msg["asks"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = datetime.strptime(msg["time"],"%Y-%m-%dT%H:%M:%S.%fZ")  + timedelta(hours = 2)
                S = [""]
                bids = msg["changes"]
                asks = "-"

        elif exchange == "Huobi":
            d_2 = pd.DataFrame({ "t": [BinanceToTime(msg["ts"])], 
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
                t_temp = BinanceToTime(int(round(time.time() * 1000)))
                d_2 = pd.DataFrame({ "t": [t_temp], 
                        "Host": [exchange], 
                        "Pair": [pair],
                        "LastUpdateID": ["-"],
                        "Bids":[msg[1]["bs"]], 
                        "Asks":[msg[1]["as"]]
                    })
                exec("globals()['" + exchange + "_" + pair.upper() + "_df_Book'] = globals()['" + exchange + "_" + pair.upper() + "_df_Book'].append(d_2)", globals(),{"d_2":d_2})
                add = 0
            else:
                t = 0
                if "b" in msg[1]:
                    bids = msg[1]["b"]
                    for element in msg[1]["b"]:
                        if float(element[2]) > t:
                            t = float(element[2])
                else:
                    bids = "-"
                if "a" in msg[1]:
                    asks = msg[1]["a"]
                    for element in msg[1]["a"]:
                        if float(element[2]) > t:
                            t = float(element[2])
                else:
                    asks = "-"
                if t == 0:
                    t = BinanceToTime(int(round(time.time() * 1000)))
                else: 
                    t = datetime.fromtimestamp(t)
                S = ["-"]
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
                print(msg, file = globals()["file_out"])
                print("MARKET - message type: Other - Market: " + exchange, file = globals()["file_out"])  
                print(msg)
                print("MARKET - message type: Other - Market: " + exchange)

def generate_new_thread(exchange, number):

    if exchange == "Bitfinex":
        client = Web_Client.Bitfinex()
        client.start_book("tETHBTC", partial(process_message_2,exchange = "Bitfinex", pair = "ethbtc" + str(number)))
        #client.start_book("tBTCUSD", partial(process_message_2,exchange = "Bitfinex", pair = "btcusd" + str(number)))
        #client.start_book("tETHUSD", partial(process_message_2,exchange = "Bitfinex", pair = "ethusd" + str(number)))
    elif exchange == "Coinbase":
        client = Web_Client.Coinbase()
        #client.start_ticker('ETH-USD', partial(process_message_2,exchange = "Coinbase", pair = "ethusd" + str(number)))
        #client.start_ticker('BTC-USD', partial(process_message_2,exchange = "Coinbase", pair = "btcusd" + str(number)))
        client.start_ticker('ETH-BTC', partial(process_message_2,exchange = "Coinbase", pair = "ethbtc" + str(number)))
    elif exchange == "Kraken":
        client = Web_Client.Kraken()
        client.start_book('ETH/XBT', partial(process_message_2,exchange = "Kraken", pair = "ethbtc" + str(number)))

    return client

def loadPair():
    groups = ["Binance_ETHBTC","Bitfinex_ETHBTC","Bitfinex_ETHBTC2","Bitfinex_ETHBTC3","Bitstamp_ETHBTC", 
            "Bithumb_ETHBTC", "Coinbase_ETHBTC","Coinbase_ETHBTC2","Coinbase_ETHBTC3","Huobi_ETHBTC", "Kraken_ETHBTC", "Kraken_ETHBTC2","Kraken_ETHBTC3"]
            #"Bitfinex_BTCUSD","Bitfinex_BTCUSD2","Bitfinex_BTCUSD3","Bitstamp_BTCUSD", "Coinbase_BTCUSD","Coinbase_BTCUSD2",
            #"Bitfinex_ETHUSD","Bitfinex_ETHUSD2","Bitfinex_ETHUSD3","Bitstamp_ETHUSD", "Coinbase_ETHUSD","Coinbase_ETHUSD2"]

    text = "global client, file_out "
    for element in groups:
        text = text + "," + element + "_df_dif"
        text = text + "," + element + "_df"
        text = text + "," + element + "_df_Book"
    exec(text, globals())
    for element in groups:

        exec(element + "_df_dif = pd.DataFrame(columns = ['t', 'Host', 'Pair', 'Seg', 'Bids', 'Asks'])", globals())
        exec(element + "_df = pd.DataFrame(columns = ['t', 'Host', 'Pair', 'Price', 'Q'])", globals())
        exec(element + "_df_Book = pd.DataFrame(columns = ['t', 'Host', 'Pair', 'LastUpdateID','Bids','Asks'])" , globals())


    client = DataFrameClient('localhost', 8086, 'root', 'root')
    script_dir = os.path.dirname(__file__)
    date = datetime.now()
    rel_path = "Logs\\log_" + str(date.date()) + "_" + str(date.hour) + str(date.minute) + str(date.second) + ".txt"
    abs_file_path = os.path.join(script_dir, rel_path)

    globals()["file_out"] = open(abs_file_path, "a")

    #if {"name":"SecondM"} not in client.get_list_database():
    #    client.create_database("SecondM")
    #else:
    #    client.drop_database("SecondM")
    #    client.create_database("SecondM")

    Binance = Web_Client.Binance()
    Binance_REST = Client.Binance()

    Bitfinex = Web_Client.Bitfinex()
    Bitfinex2 = generate_new_thread("Bitfinex", "")
    Bitfinex_REST = Client.Bitfinex()

    Bithumb = Web_Client.Bithumb()
    Bithumb_REST = Client.Bithumb()

    Bitstamp = Web_Client.Bitstamp()

    Coinbase = Web_Client.Coinbase()
    Coinbase2 = generate_new_thread("Coinbase", "")

    Kraken = Web_Client.Kraken()
    Kraken2 = generate_new_thread("Kraken", "")

    Huobi = Web_Client.Huobi()


    Binance.start_trade_socket('ethbtc', partial(process_message,exchange = "Binance", pair = "ethbtc"))
    Bitfinex.start_trades("tETHBTC", partial(process_message,exchange = "Bitfinex", pair = "ethbtc"))    
    Bithumb.start_trade('ETH-BTC', partial(process_message,exchange = "Bithumb", pair = "ethbtc"))
    Bitstamp.start_ticker('ethbtc', partial(process_message,exchange = "Bitstamp", pair = "ethbtc"))
    Coinbase.start_matches('ETH-BTC', partial(process_message,exchange = "Coinbase", pair = "ethbtc"))
    Huobi.start_trade('ethbtc', partial(process_message,exchange = "Huobi", pair = "ethbtc"))
    Kraken.start_trade('ETH/XBT', partial(process_message,exchange = "Kraken", pair = "ethbtc"))

    #time.sleep(1)

    #Bitfinex.start_trades("tBTCUSD", partial(process_message,exchange = "Bitfinex", pair = "btcusd"))
    #Bitstamp.start_ticker('btcusd', partial(process_message,exchange = "Bitstamp", pair = "btcusd"))
    #Coinbase.start_matches('BTC-USD', partial(process_message,exchange = "Coinbase", pair = "btcusd"))

    #time.sleep(1)

    #Bitfinex.start_trades("tETHUSD", partial(process_message,exchange = "Bitfinex", pair = "ethusd"))
    #Bitstamp.start_ticker('ethusd', partial(process_message,exchange = "Bitstamp", pair = "ethusd"))
    #Coinbase.start_matches('ETH-USD', partial(process_message,exchange = "Coinbase", pair = "ethusd"))

    #############################################
    #############################################
    #############################################
    #############################################

    Binance.start_depth_socket("ethbtc",partial(process_message_2,exchange = "Binance", pair = "ethbtc"))
    Bithumb.start_order_book('ETH-BTC', partial(process_message_2,exchange = "Bithumb", pair = "ethbtc"))
    Bitstamp.start_detailOrder('ethbtc', partial(process_message_2,exchange = "Bitstamp", pair = "ethbtc"))
    Huobi.start_depth('ethbtc', partial(process_message_2,exchange = "Huobi", pair = "ethbtc"))

    #time.sleep(1)

    #Bitstamp.start_detailOrder('btcusd', partial(process_message_2,exchange = "Bitstamp", pair = "btcusd"))

    #time.sleep(1)

    #Bitstamp.start_detailOrder('ethusd', partial(process_message_2,exchange = "Bitstamp", pair = "ethusd"))


    Binance.start()
    Bithumb.start()
    Bitstamp.start()
    Coinbase.start()
    Bitfinex.start()
    Huobi.start()
    Kraken.start()

    Kraken2.start()
    Coinbase2.start()
    Bitfinex2.start()

    for i in range(6):
        try:
            _msg_ = Binance_REST.depth("ETHBTC", limit = 1000)
            t_temp = BinanceToTime(int(round(time.time() * 1000)))
            d_2 = pd.DataFrame({ "t": [t_temp], 
                        "Host": ["Binance"], 
                        "Pair": ["ethbtc"],
                        "LastUpdateID": [_msg_["lastUpdateId"]],
                        "Bids":[_msg_["bids"]], 
                        "Asks":[_msg_["asks"]]
                    })
            var = globals()
            var["d_2"] = d_2
            exec("Binance_ETHBTC_df_Book = Binance_ETHBTC_df_Book.append(d_2)", var)
        except Exception as e:
            print("EXCEPTION!", file = file_out)
            print("EXCEPTION!")
            print(e, file = file_out)
            print(e)
        time.sleep(4)
    status = 3
    # TO DO respect to time.now, not for loop
    for i in range(4500):
        if status == 3:
            try:
                Bitfinex3.close()
                Kraken3.close()
                Coinbase3.close()
            except:
                pass
            time.sleep(1)
            Bitfinex3 = generate_new_thread("Bitfinex", "2")
            Kraken3 = generate_new_thread("Kraken", "2")
            Coinbase3 = generate_new_thread("Coinbase", "2")
            time.sleep(3)
            Bitfinex3.start()            
            Kraken3.start()   
            Coinbase3.start()
            status = 4
        elif status == 4:
            try:
                Bitfinex4.close()
                Kraken4.close()
                Coinbase4.close()
            except:
                pass
            time.sleep(1)
            Bitfinex4 = generate_new_thread("Bitfinex", "3")
            Kraken4 = generate_new_thread("Kraken", "3")
            Coinbase4 = generate_new_thread("Coinbase", "3")
            time.sleep(3)
            Kraken4.start()
            Bitfinex4.start()  
            Coinbase4.start()
            status = 2
        elif status == 2:
            try:
                Bitfinex2.close()
                Kraken2.close()
                Coinbase2.close()
            except:
                pass
            time.sleep(1)
            Bitfinex2 = generate_new_thread("Bitfinex", "")
            Kraken2 = generate_new_thread("Kraken", "")
            Coinbase2 = generate_new_thread("Coinbase", "")
            time.sleep(1)
            Bitfinex2.start()
            Kraken2.start()    
            Coinbase2.start()
            status = 3

        if i % 5 == 0:
            for element in groups:  
                print("\n----------------")
                print("\n----------------", file = globals()["file_out"])
                print("CHECKING DATAFRAMES - " + str(i) + ": " +  str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
                print("CHECKING DATAFRAMES - " + str(i) + ": " +  str(BinanceToTime(int(round(time.time() * 1000)))))
                print("---------------- \n")
                print("---------------- \n", file = globals()["file_out"])
                ldict = {}
                var = globals()
                exec("df_temp = " + element + "_df",var, ldict)
                df_temp = ldict["df_temp"]
                exec("df_Book_temp = " + element + "_df_Book",var, ldict)
                df_Book_temp = ldict["df_Book_temp"]
                exec("df_dif_temp = " + element + "_df_dif",var, ldict)
                df_dif_temp = ldict["df_dif_temp"]

                var.update(ldict)

                print(element + " : " + str(len(df_temp)) + "; " + str(len(df_Book_temp)) + "; " + str(len(df_dif_temp)))
                print(element + " : " + str(len(df_temp)) + "; " + str(len(df_Book_temp)) + "; " + str(len(df_dif_temp)), file = globals()["file_out"])


                if len(df_Book_temp)>2000:
                    print("-------------------------------", file = globals()["file_out"])
                    print("-------------------------------")
                    df_Book_temp = df_Book_temp.set_index("t")
                    print("WRITING " + str(len(df_Book_temp)) + " lines in Book FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
                    print("WRITING " + str(len(df_Book_temp)) + " lines in Book FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
                    try:
                        client.write_points(df_Book_temp.assign(Batch_ID = [i for i in range(len(df_Book_temp))]),"Book",time_precision = "n", database = "SecondM", tag_columns = ['Host', 'Pair', 'Batch_ID'], batch_size = 1000)
                        exec(element + "_df_Book = " + element + "_df_Book.iloc[len(df_Book_temp):]", var)
                    except Exception as e:
                        print("DATABASE PROBLEM!")
                        print(e)
                    print("-------------------------------", file = globals()["file_out"])
                    print("-------------------------------")

                if len(df_dif_temp)>2000:
                    print("-------------------------------", file = globals()["file_out"])
                    print("-------------------------------")
                    df_dif_temp = df_dif_temp.set_index("t")
                    print("WRITING " + str(len(df_dif_temp)) + " lines in difBook FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
                    print("WRITING " + str(len(df_dif_temp)) + " lines in difBook FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
                    try:
                        client.write_points(df_dif_temp.assign(Batch_ID = [i for i in range(len(df_dif_temp))]),"difBook",time_precision = "n", database = "SecondM", tag_columns = ['Host', 'Pair', 'Batch_ID'], batch_size = 1000)
                        exec(element +  "_df_dif = " + element + "_df_dif.iloc[len(df_dif_temp):]", var)
                    except Exception as e:
                        print("DATABASE PROBLEM!")
                        print(e)
                    print("-------------------------------", file = globals()["file_out"])
                    print("-------------------------------")

                if len(df_temp)>2000:
                    print("-------------------------------", file = globals()["file_out"])
                    print("-------------------------------")
                    df_temp = df_temp.set_index("t")
                    print("WRITING " + str(len(df_temp)) + " lines in Price FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
                    print("WRITING " + str(len(df_temp)) + " lines in Price FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
                    try:
                        client.write_points(df_temp.assign(Batch_ID = [i for i in range(len(df_temp))]),"Price", time_precision = "n", database = "SecondM", tag_columns = ['Host', 'Pair', 'Batch_ID'], batch_size = 1000)
                        exec(element +  "_df = " + element + "_df.iloc[len(df_temp):]", var)
                    except Exception as e:
                        print("DATABASE PROBLEM!")
                        print(e)
                    print("-------------------------------", file = globals()["file_out"])
                    print("-------------------------------")
            try:        
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
            except Exception as e:
                print("EXCEPTION!", file = file_out)
                print("EXCEPTION!")
                print(e, file = file_out)
                print(e)


        time.sleep(24)
    
    Binance.close()
    Bitfinex.close()
    Bitfinex2.close()
    Bitfinex3.close()
    Bitfinex3.close()
    Bithumb.close()
    Bitstamp.close()
    Coinbase.close()
    Coinbase2.close()
    Coinbase3.close()
    Coinbase4.close()
    Huobi.close()
    Kraken.close()
    Kraken2.close()
    Kraken3.close()
    Kraken4.close()

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

        print("\n-------------------------------", file = globals()["file_out"])
        print("\n-------------------------------")
        df_Book_temp = df_Book_temp.set_index("t")
        print("WRITING " + str(len(df_Book_temp)) + " lines in Book FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
        print("WRITING " + str(len(df_Book_temp)) + " lines in Book FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
        try:
            client.write_points(df_Book_temp.assign(Batch_ID = [i for i in range(len(df_Book_temp))]),"Book",time_precision = "n", database = "SecondM", tag_columns = ['Host', 'Pair', 'Batch_ID'], batch_size = 100)
            df_Book_temp.to_csv(element + "_df_Book")
        except:
            df_Book_temp.to_csv(element + "_df_Book")

        df_dif_temp = df_dif_temp.set_index("t")
        print("WRITING " + str(len(df_dif_temp)) + " lines in difBook FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
        print("WRITING " + str(len(df_dif_temp)) + " lines in difBook FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
        try:
            client.write_points(df_dif_temp.assign(Batch_ID = [i for i in range(len(df_dif_temp))]),"difBook",time_precision = "n", database = "SecondM", tag_columns = ['Host', 'Pair', 'Batch_ID'], batch_size = 100)
            df_dif_temp.to_csv(element + "_df_dif")
        except:
            df_dif_temp.to_csv(element + "_df_dif")


        df_temp = df_temp.set_index("t")
        print("WRITING " + str(len(df_temp)) + " lines in Price FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))), file = globals()["file_out"])
        print("WRITING " + str(len(df_temp)) + " lines in Price FROM " + element + ": " + str(BinanceToTime(int(round(time.time() * 1000)))))
        try:
            client.write_points(df_temp.assign(Batch_ID = [i for i in range(len(df_temp))]),"Price", time_precision = "n", database = "SecondM", tag_columns = ['Host', 'Pair', 'Batch_ID'], batch_size = 100)
            df_temp.to_csv(element + "_df")
        except:
            df_temp.to_csv(element + "_df")
        print("------------------------------- \n", file = globals()["file_out"])
        print("------------------------------- \n")
    
    print("..END..", file = globals()["file_out"])
    print("..END..")
    file_out.close()

