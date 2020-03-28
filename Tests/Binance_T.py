from API.Main.Client import Client, Web_Client

from influxdb import InfluxDBClient
from datetime import datetime
import time
from twisted.internet import reactor

try:
    f=open("API.txt", "r")
    
    api_key = f.readline().strip()
    secret_key = f.readline().strip()
    f.close()
except:
    api_key = ""
    secret_key = ""

a = Client.Binance(api_key, secret_key)

#client = InfluxDBClient('localhost', 8086, 'root', 'root', 'Markets')
#

print("First we ping\n")
#print(a.ping())

print("Now we check server time")
check = datetime.fromtimestamp(a.get_server_time()["serverTime"]/1000).strftime('%Y-%m-%dT%H:%M:%SZ')
print(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ'))
json_body = [
    {
        "measurement": "serverTime",
        "tags": {
            "host": "Binance"
        },
        "time": check,
        "fields": {
            "LocalTime": datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
        }
    }
]

#client.write_points(json_body)

print(datetime.fromtimestamp(time.time()))
print("And now the Exchange info")
print(a.get_exchange_info())

print("Order book for ETHBTC")
print(a.depth("ETHBTC",5))

print("Last trades")
print(a.trades("ETHBTC"))

print("Historical trades")
print(a.htrades("ETHBTC"))

print("Aggregate trades")
print(a.aggtrades("ETHBTC"))

print("Klines")
print(a.klines("ETHBTC", "1m"))

print("avgprice")
print(a.avgPrice("ETHBTC"))

print("tkr24")
print(a.tkr24("ETHBTC"))

print("price")
print(a.price("ETHBTC"))

print("bookTicker")
print(a.avgPrice("ETHBTC"))


# WEBSOCKETS
def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

    # do something

b = Web_Client.Binance()
# start any sockets here, i.e a trade socket
b.start_trade_socket('bnbbtc', process_message)
b.start_aggtrade_socket('bnbbtc', process_message)
b.start_kline_socket('bnbbtc', process_message)
b.start_miniticker_socket('BNBBTC', process_message)
b.start_symbol_ticker_socket('BNBBTC', process_message)
b.start_ticker_socket(process_message)
b.start_symbol_book_ticker_socket('BNBBTC', process_message)
b.start_book_ticker_socket(process_message)
b.start_depth_socket('BNBBTC', process_message)
b.start_diff_depth_socket('BNBBTC', process_message)
#then start the socket manager
b.start()
#print(b.is_alive())
time.sleep(10)
b.close()
a.close()