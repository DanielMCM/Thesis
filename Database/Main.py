from  Database.Functions.Loaddb import loadPair
import time

def main(): 
    try:
        loadPair()
    except Exception as e:
        print(e) 
        print("RESTARTING EVERYTHING!!!!!!!!! " + time.time())
        main()

main()