from datetime import datetime
import pandas as pd

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
    global df
    try:
        if exchange == "Binance":
            global df_Bindif
            ["t", "Host", "Pair", "First", "Last", "Bids", "Asks"]
            print(msg)
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
        #print(d)
    except:
        print("message type: Other")
        #print(msg["params"]["message"][0]["exec_date"][:26])
        #print("-----")
        #print(datetime.strptime(msg["params"]["message"][0]["exec_date"][:26],"%Y-%m-%dT%H:%M:%S.%f"))
        ##print(msg[1][0][2])
        #print(BinanceToTime(int(round(time.time() * 1000))))

