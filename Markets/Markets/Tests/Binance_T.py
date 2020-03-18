from Markets.API.Main.Client import Client, Web_Client

from influxdb import InfluxDBClient
from datetime import datetime
import time
from twisted.internet import reactor

api_key = ""

secret_key = ""

a = Client.Binance(api_key, secret_key)
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'Markets')


print("First we ping\n")
print(a.ping())

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

client.write_points(json_body)

print(datetime.fromtimestamp(time.time()))

print("And now the Exchange info")
print(a.get_exchange_info())


print("Order book for ETHBTC")
print(a.depth("ETHBTC",5))



# WEBSOCKETS
def process_message(msg):
    print("message type: {}".format(msg['e']))
    print(msg)
    # do something

b = Web_Client.Binance()
# start any sockets here, i.e a trade socket
conn_key = b.start_trade_socket('BNBBTC', process_message)
print(conn_key)
# then start the socket manager
b.start()
print(b.is_alive())
time.sleep(10)
b.close()
a.close()