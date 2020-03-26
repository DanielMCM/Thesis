from API.Main.Client import Client, Web_Client

from datetime import datetime
import time
from twisted.internet import reactor

api_key = ""
secret_key = ""

cl = Client.Bitfinex(api_key, secret_key)

print("TICKERS\n")
print(cl.tickers(["tBTCUSD"]))

print("TICKER\n")
print(cl.ticker("tBTCUSD"))

print("Book\n")
print(cl.book("tBTCUSD"))

print("STATUS\n")
print(cl.status())

print("CANDLES\n")
print(cl.candles("tBTCUSD"))

print("CONFIG\n")
print(cl.configs())

print("STATUS_2\n")
print(cl.status_2())

print("LIQUIDATION\n")
print(cl.liquidation())

print("LEADERBOARD\n")
print(cl.leaderboards("tBTCUSD"))

def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

    # do something

b = Web_Client.Bitfinex()
# start any sockets here, i.e a trade socket
b.start_ticker('tBTCUSD', process_message)
b.start_trades('tBTCUSD', process_message)
b.start_book('tBTCUSD', process_message)
b.start_raw_book('tBTCUSD', process_message)
b.start_candles('tBTCUSD', process_message)
b.start_status(process_message)

b.start()
print(b.is_alive())
time.sleep(20)
b.close()
b.close()