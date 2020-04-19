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



