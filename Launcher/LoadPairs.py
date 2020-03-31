from influxdb import InfluxDBClient
from API.Main.Client import Client
import pandas as pd


#client = InfluxDBClient('localhost', 8086, 'root', 'root')
Binance = Client.Binance()
Bitfinex = Client.Bitfinex()
BitFlyer = Client.BitFlyer()
Bithumb = Client.Bithumb()
Bitstamp = Client.Bitstamp()
Coinbase = Client.Coinbase()
Huobi = Client.Huobi()
Kraken = Client.Kraken()

#if "MarketPairs" in client.get_list_database():
#    client.drop_database("MarketPairs")
#    client.create_database('MarketPairs')
#else:
#    client.create_database('MarketPairs')


#client = InfluxDBClient('localhost', 8086, 'root', 'root', "MarketPairs")

list = []
for symbols in Binance.get_exchange_info()["symbols"]:
    list.append(["Binance",symbols["baseAsset"] + "_" + symbols["quoteAsset"]])

for symbol in Bitfinex.configs()[0]:
    list.append(["Bitfinex", symbol])

for symbol in BitFlyer.markets():
    list.append(["BitFlyer", symbol["product_code"]])

for symbol, data in Bithumb.ticker("ALL")["data"].items():
    if symbol != "date":
        list.append(["Bithumb", symbol + "_" + "KRW"])

for symbol in Bitstamp.pairs_info():
    list.append(["Bitstamp", symbol["name"]])

for symbol in Coinbase.product():
    list.append(["Coinbase",symbol["id"]])

for symbol in Huobi.symbols()["data"]:
    #print(symbol)
    #print(symbol.get("uote-currency"))
    list.append(["Huobi", symbol.get("base-currency") + "_" + symbol.get("quote-currency")])

for symbol, data in Kraken.AssetPairs()["result"].items():
    if data.get("wsname") == None:
        list.append(["Kraken",symbol])
    else:    
        list.append(["Kraken", data.get("wsname")])

out = pd.DataFrame(list)

out.to_csv(r'./Pairs.csv', index = False)