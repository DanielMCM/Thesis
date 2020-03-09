from Markets.Main.Client import Client
from datetime import datetime


a = Client.Bithumb()

print("We get the first pair!\n")
print(a.ticker("BTC", "KRW"))