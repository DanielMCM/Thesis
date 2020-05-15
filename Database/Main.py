from  Database.Functions.Loaddb import loadPair
import time

def main(): 
    try:
        loadPair()
    except Exception as e:
        print(e) 
        print("RESTARTING EVERYTHING!!!!!!!!! " + str(time.time()))
        main()

main()