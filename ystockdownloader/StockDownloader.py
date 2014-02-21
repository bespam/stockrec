from SymbolDownloader import SymbolDownloader
#from Stock import Stock
import pdb

class StockDownloader(SymbolDownloader):
    def __init__(self):
        SymbolDownloader.__init__(self, "Stock")
    
    def decodeSymbolsContainer(self, symbolsContainer):
        symbols = []
        for row in symbolsContainer:
            ticker = row.contents[0].string
            companyName = row.contents[1].string
            type = row.contents[3].string
            categoryName = row.contents[4].string
            categoryNr = 0
            if(categoryName != None):
                categoryNr = row.contents[4].a.get('href').split("/").pop().split(".")[0]
            exchange = row.contents[5].string
            if exchange is None: continue #none types
            #exporting US symbols
            #limit securities only with USD currency
            exchanges = ["PNK","NYQ","NGM","ASE","NCM","OBB","NMS"]
            if exchange.encode() in exchanges:
                symbols.append([ticker, companyName, exchange, categoryName, categoryNr])
        return symbols

    def getRowHeader(self):
        return SymbolDownloader.getRowHeader(self) + ["categoryName", "categoryNr"]
