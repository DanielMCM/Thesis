from Markets.API.Main.Client import Client
from influxdb import InfluxDBClient
from datetime import datetime
import time

a = Client.Binance()
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