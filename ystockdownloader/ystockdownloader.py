#A modified version of https://github.com/Benny-/Yahoo-ticker-symbol-downloader

import sys
import json
import csv
from time import sleep
import os
import pdb
import shutil
import gc

from StockDownloader import StockDownloader

sys.setrecursionlimit(10000) # Do not remove this line. It contains magic.

# --------------------------------------------NOTE: http://finance.yahoo.com/lookup/?s=-&m=US&t=s  can give all the stocks. (FIX later)


def loadDownloader():
    with open("downloader.json", "r") as file:
        return json.load(file);

def saveDownloader(downloader):
    with open("downloader_new.json","w") as file:
        s = [downloader.symbols, downloader.nextq, downloader.items,downloader.totalItems]
        json.dump(s, file)
    shutil.copyfile("downloader_new.json","downloader.json")
    os.remove("downloader_new.json")
        

def main():
    #directory of the current module
    module_dir = os.path.dirname(__file__)
    if module_dir != "": module_dir = module_dir + "\\"
    
    downloader = StockDownloader()
    print("Checking if we can resume a old download session")
    try:
        s = loadDownloader()
        downloader.symbols = s[0]
        downloader.nextq = s[1]
        downloader.items = s[2]
        downloader.totalItems = s[3]
        print("Downloader found on disk, resuming")
    except:
        print("No old downloader found on disk")

    try:
        if not downloader.isDone():
            print("Downloading " + downloader.type)
            symbols = downloader.fetchNextSymbols()
            lastSaveQuery = downloader.getQuery()
            while not downloader.isDone():
                print("Progress-- " +
                        " Queries: " + str(downloader.getQueryNr()) + "/" + str(downloader.getTotalQueries()) +
                        " Items in query: " + str(downloader.getItems()) + "/" + str(downloader.getTotalItems()) +
                        " collected " + downloader.type + " data: " + str(downloader.getCollectedSymbolsSize())
                        +"           \r"),
                symbols = downloader.fetchNextSymbols()
                #print("Got " + str(len(symbols)) + " downloaded symbols")
                if(len(symbols)>2):
                    #print (str(symbols[0]))
                    #print (str(symbols[1]))
                    #print ("..ect")
                    #print ("")
                    pass
                if downloader.getQuery() != lastSaveQuery:
                    lastSaveQuery = downloader.getQuery()
                    #print ("Saving downloader to disk..."+"                                                                    \r"),
                    saveDownloader(downloader)
                else:
                    sleep(5) # We dont wish to overload the server.
                gc.collect()
    except Exception as ex:
        raise
    except KeyboardInterrupt as ex:
        print("Suspending downloader to disk")
        raise

    if downloader.isDone():
        print("Exporting "+downloader.type+" symbols")
        with open(module_dir + 'stocks_info.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile,lineterminator='\n')
            csvwriter.writerow(downloader.getRowHeader())
            for symbol in downloader.getCollectedSymbols():
                if len(symbol[0].split(" ")) == 1:
                    try:
                        csvwriter.writerow(symbol)
                    except:
                        pass  
        os.remove("downloader.json")
if __name__ == "__main__":
    main()
