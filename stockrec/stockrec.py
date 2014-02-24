import os
import pandas as pd
import numpy as np
from scipy.sparse import *
import scipy as sp
import csv
from ystockquote import ystockquote
import math
import copy
import json
from matplotlib import pyplot as plt
from collections import defaultdict, OrderedDict
import pdb
from sys import stdout
import couchdbkit as couch
import restkit
from restkit import BasicAuth
import time, os.path
import datetime
import sys
import shutil
import pickle


def analyze(ystockquote_run, stockconvert_run, couchDB_push):
    #directory of the current module
    module_dir = os.path.dirname(__file__)
    if module_dir != "": module_dir = module_dir + "\\"
    try:
        # define data_directory and get date_label
        # roll back 7 hours to have the late analysis to be saved as the previous day
        day = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours=7),'%Y-%m-%d')

        data_dir = module_dir + "..\\data\\"

        print("Initiating analysis, "+"Time: " + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())))
        #loading csv file with all the stocks
        stocksTicks = pd.read_csv(data_dir+"stocks_info.csv")
        print("stocks_info.csv file was loaded")
        
        stocksTicks = stocksTicks.set_index('Ticker')
        del stocksTicks['categoryNr']
        stocksTicks.columns = ['Name','Exchange','Industry']
        #fix error with "Regional - Southwest Banks" (double space)
        stocksTicks.loc[stocksTicks['Industry'] == 'Regional - Southwest  Banks','Industry'] ='Regional - Southwest Banks'

        #load Yahoo finance sectors and industries
        stockSectors = pd.read_csv(module_dir + "sectors.csv")
        print("sectors.csv file was loaded")
        stockSectors = stockSectors.set_index('Sector')
        stockIndustries = pd.read_csv(module_dir + "industries.csv")
        print("industries.csv file was loaded")
        stockIndustries = stockIndustries.set_index('Industry')

        #load list of industries and sectors
        stockSectorsIndustries = pd.read_csv(module_dir + "sectors_industries.csv")
        print("sectors_industries.csv file was loaded")
        stockSectorsIndustries = stockSectorsIndustries.set_index('Industry')
        stocksAll = stocksTicks.join(stockSectorsIndustries,on=['Industry'])
        #add Sector number
        stocksAll = stocksAll.join(stockSectors,on=['Sector'])
        #add Industry number
        stocksAll = stocksAll.join(stockIndustries,on=['Industry'])
        #add Exchange number
        exchanges = list(set(stocksAll["Exchange"].dropna().values.tolist()))
        exchange_numbers = range(len(exchanges))
        stockExchanges = pd.DataFrame(zip(exchanges,exchange_numbers),columns = ["Exchange","Exchange_number"]).set_index("Exchange")
        stocksAll = stocksAll.join(stockExchanges,on=['Exchange'])
        print("stocksAll dataframe was created")
                
        # reading profile file
        file = data_dir+"stocks_profiles.json"
        f = open(file,"r") 
        profile = json.load(f)
        employees = []
        address = []
        country = []
        summary = []
        indices = []
        indices_numbers =[]        
        # calculate indices_numbers from indices names
        indices_set = set([])
        for s in profile.values():
            indices_set.update(s["indexMemberships"])
        indices_set.remove(u'N/A')
        indices_list = [u'N/A']
        indices_list.extend(list(indices_set))
        indices_dict = dict(zip(list(indices_list),range(len(indices_list))))
        for tick in stocksAll.index.values:
            employees.append(profile[tick]["fullTimeEmployees"])
            address.append(profile[tick]["address"])
            country.append(profile[tick]["country"])
            summary.append(profile[tick]["businessSummary"])
            indices.append(profile[tick]["indexMemberships"])
            indices_numbers.append([indices_dict[s] for s in profile[tick]["indexMemberships"]])  
        stocksAll["Employees"] = employees
        stocksAll["Summary"] = summary
        stocksAll["Country"] = country
        #add Country number
        countries = list(set(country))
        country_numbers = range(len(countries))
        stocksAll["Country_number"] = [country_numbers[countries.index(c)] for c in country]
        stocksAll["Address"] = address
        stocksAll["Indices"] = indices
        stocksAll["Indices_numbers"] = indices_numbers       
        
        # reading summary_tfidf.json
        file = data_dir + "summary_tfidf.json"
        f = open(file,"r") 
        summary_tfidf = json.load(f)
        
        time_stamp = ""
        #loop through all available stocks
        def getAllInfo():
            info = {}
            print("total stocks: "+ str(len(stocksAll.index.values)))
            #split in groups of 200
            n = 200
            symbol_groups = [ stocksAll.index.values[i:i+n] for i in range(0, len(stocksAll.index.values), n) ]
            i = 0
            n_errors = 0
            for symbols in symbol_groups:
                attempt = 0
                while attempt < 5:
                    res = ystockquote.get_all(symbols.tolist())
                    if "error" not in res.keys(): 
                        for tick in res.keys():
                            info[tick] = res[tick]
                        attempt = 10
                        print("\r" +" "*110),
                        print("\r"+str(i)+ "-"+str(i+n)+" stocks were loaded, n_errors: "+ str(n_errors)),
                    else:
                        n_errors = n_errors + 1
                        print("\r" +" "*110), 
                        print("\r"+str(i)+ "-"+str(i+n)+", error:" + str(res['error'])+ ": waiting "  + str(attempt*5)+ " minutes"),
                        attempt = attempt + 1
                        time.sleep(300*attempt)
                    time.sleep(180) #3 min
                #check if finished with the error
                if attempt != 10:
                    print("\r" +" "*110)
                    print("Critical errors during ystockquote")
                    pdb.set_trace()
                i = i + 200
            #dump stockInfo to file
            f = open(module_dir + "ystockquote\\stocks_quotes.json","w")
            json.dump(info,f)
            #time_stamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
            
        def loadAllInfo():
            file = module_dir + "ystockquote\\stocks_quotes.json"
            f = open(file,"r") 
            info = json.load(f)
            t = os.path.getmtime(file)
            time_stamp = datetime.datetime.fromtimestamp(t).strftime('%m-%d-%Y')
            return (info,time_stamp)
        
        if ystockquote_run == True: 
            print("stock stats are being reloaded from Yahoo")
            getAllInfo() # comment this out once data is loaded
            print("\r")
            print("stock stats were reloaded from Yahoo and saved to stocksInfo.json")
        else:
            print("stock reload was skipped")
        # copy stocks_quotes.json to common data dir
        quotes_file = data_dir + "stocks_quotes_"+day+".json"
        shutil.copyfile(module_dir + "ystockquote\\stocks_quotes.json",quotes_file)
        print("stocks_quotes.json file was copied to "+quotes_file)
        
        stocksInfo,time_stamp = loadAllInfo()
        print("stock stats were loaded from stocksInfo.json")

        #get info and normalize
        def stockConvert(tick):
            stock = copy.deepcopy(stocksInfo[tick])
            stock_new = OrderedDict()
            #converting and normalizing stock variables
            
            
            
            #sector number
            sector_number = stocksAll.loc[tick]["Sector_number"]
            if np.isnan(sector_number): #check for NAN
                stock_new["sector_number"] = {"original":'N/A', "converted":stocksAll.loc[tick]["Sector_number"]}
            else:
                stock_new["sector_number"] = {"original":stocksAll.loc[tick]["Sector"], "converted":stocksAll.loc[tick]["Sector_number"]}
            
            #industry number
            industry_number = stocksAll.loc[tick]["Industry_number"]
            if np.isnan(industry_number): #check for NAN:
                stock_new["industry_number"] = {"original":'N/A', "converted":stocksAll.loc[tick]["Industry_number"]}
            else:
                stock_new["industry_number"] = {"original":stocksAll.loc[tick]["Industry"], "converted":stocksAll.loc[tick]["Industry_number"]}
            
            #exchange number
            stock_new["exchange_number"] = {"original":stocksAll.loc[tick]["Exchange"], "converted":stocksAll.loc[tick]["Exchange_number"]}

            #country number
            country_number = stocksAll.loc[tick]["Country_number"]
            if np.isnan(country_number): #check for NAN:
                stock_new["country_number"] = {"original":'N/A', "converted":stocksAll.loc[tick]["Country_number"]}
            else:
                stock_new["country_number"] = {"original":stocksAll.loc[tick]["Country"], "converted":stocksAll.loc[tick]["Country_number"]}   
            
            #index_memberships        
            indices = [a.encode("ascii","xmlcharrefreplace") for a in stocksAll.loc[tick]["Indices"]]
            indices_numbers = float("".join([str(s) for s in stocksAll.loc[tick]["Indices_numbers"]]))
            if indices_numbers == 0.0:           
                stock_new['ind_membership'] = {"original":'N/A', "converted":0}
            else:
                stock_new['ind_membership'] = {"original":", ".join(indices), "converted":indices_numbers} #converting to float then convert back   

            
            #employees
            employees = stocksAll.loc[tick]["Employees"]
            if employees == "N/A":           
                stock_new['employees_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['employees_log'] = {"original":employees, "converted":math.log(float(employees),10)}
                  
            
            #stock last_trade_price
            last_trade_price = stock['last_trade_price']
            if last_trade_price in ['N/A',"-"]  or float(last_trade_price) <= 0:
                stock_new['last_trade_price_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['last_trade_price_log'] = {"original":last_trade_price, "converted":math.log(abs(float(last_trade_price)),10)}

            #Change in %
            percent_change = stock['change_percent_change'].replace('"','').split(" ")[2]
            if percent_change in ['N/A',"-"]:
                stock_new['percent_change_sqrt'] = {"original":'N/A', "converted":np.nan}
            else:
                percent_change = percent_change.split("%")[0].encode()
                stock_new['percent_change_sqrt'] = {"original":percent_change, "converted":math.copysign(math.sqrt(abs(float(percent_change))),float(percent_change))}            
     
            #market Capitalization
            market_cap = stock['market_cap']
            if market_cap in ['N/A',"-"]: 
                stock_new['market_cap_log'] = {"original":'N/A', "converted":np.nan}
            elif market_cap[-1] == 'T': 
                stock_new['market_cap_log'] = {"original":market_cap, "converted":math.copysign(math.log(abs(float(market_cap[0:-1]))*1e12,10),float(market_cap[0:-1]))}
            elif market_cap[-1] == 'B': 
                stock_new['market_cap_log'] = {"original":market_cap, "converted":math.copysign(math.log(abs(float(market_cap[0:-1]))*1e9,10),float(market_cap[0:-1]))}
            elif market_cap[-1] == 'M':
                stock_new['market_cap_log'] = {"original":market_cap, "converted":math.copysign(math.log(abs(float(market_cap[0:-1]))*1e6,10),float(market_cap[0:-1]))}
            elif market_cap[-1] == 'K': 
                stock_new['market_cap_log'] = {"original":market_cap, "converted":math.copysign(math.log(abs(float(market_cap[0:-1]))*1e3,10),float(market_cap[0:-1]))}        
            else:
                stock_new['market_cap_log'] = {"original":market_cap, "converted":math.copysign(math.log(abs(float(market_cap)),10),float(market_cap))}

            #Volume
            volume = stock['volume']
            if volume in ['N/A',"-"]: 
                stock_new['volume_log'] = {"original":'N/A', "converted":np.nan}
            elif float(volume) == 0.0:
                stock_new['volume_log'] = {"original":volume, "converted":0}
            else:
                stock_new['volume_log'] = {"original":volume, "converted":math.log(abs(float(volume)),10)}
            
            #Average daily volume
            average_daily_volume = stock['average_daily_volume']
            if average_daily_volume in ['N/A',"-"]: 
                stock['average_daily_volume_log'] = {"original":'N/A', "converted":np.nan} 
            elif float(average_daily_volume) == 0.0:
                stock_new['average_daily_volume_log'] = {"original":average_daily_volume, "converted":0}
            else:
                stock_new['average_daily_volume_log'] = {"original":average_daily_volume, "converted":math.log(abs(float(average_daily_volume)),10)}
            
            #Fifty_day_moving_avg
            fifty_sma = stock['fifty_sma']
            if fifty_sma in ['N/A',"-"] or float(fifty_sma) <= 0:
                stock_new['fifty_sma_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['fifty_sma_log'] = {"original":fifty_sma, "converted":math.log(abs(float(fifty_sma)),10)}
               
            #Two hundred_day_moving_avg
            two_hundred_sma = stock['twohundred_sma']
            if two_hundred_sma in ['N/A',"-"]  or float(two_hundred_sma) <= 0:
                stock_new['two_hundred_sma_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['two_hundred_sma_log'] = {"original":two_hundred_sma, "converted":math.log(abs(float(two_hundred_sma)),10)}

            #Fifty_two_week_high
            fifty_two_week_high = stock['fiftytwo_week_high']
            if fifty_two_week_high in ['N/A',"-"] or float(fifty_two_week_high) <= 0:
                stock_new['fifty_two_week_high_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['fifty_two_week_high_log'] = {"original":fifty_two_week_high, "converted":math.log(abs(float(fifty_two_week_high)),10)}
            
            #Fifty_two_week_low
            fifty_two_week_low = stock['fiftytwo_week_low']
            if fifty_two_week_low in ['N/A',"-"] or float(fifty_two_week_low) <= 0:
                stock_new['fifty_two_week_low_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['fifty_two_week_low_log'] = {"original":fifty_two_week_low, "converted":math.log(abs(float(fifty_two_week_low)),10)}

            #Earning per share
            eps = stock['eps']
            if eps in ['N/A',"-"]:
                stock_new['eps'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['eps'] = {"original":eps, "converted":float(eps)}
     
            #Price earnings ratio    
            pe = stock['pe']
            if pe == 'N/A' in ['N/A',"-"]  or float(pe) <= 0:
                stock_new['pe_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['pe_log'] = {"original":pe, "converted":math.copysign(math.log(abs(float(pe)),10),float(pe))}
                
            #Price earnings growth ratio (PEG)
            peg = stock['peg']
            if peg in['N/A',"-"]  or float(peg) <= 0:
                stock_new['peg_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['peg_log'] = {"original":peg, "converted":math.copysign(math.log(abs(float(peg)),10),float(peg))}
                
            #Book value
            book_value = stock['book_value']
            if book_value in ['N/A',"-"]: 
                stock_new['book_value'] = {"original":'N/A', "converted":np.nan} 
            else:
                stock_new['book_value'] = {"original":book_value, "converted":float(book_value)}
            
            #Price book ratio
            price_book_ratio = stock['price_book']
            if price_book_ratio in ['N/A',"-"]  or float(price_book_ratio) <= 0:
                stock_new['price_book_ratio_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['price_book_ratio_log'] = {"original":price_book_ratio, "converted":math.log(abs(float(price_book_ratio)),10)}

            #Price sales ratio
            price_sales_ratio = stock['price_sales']
            #print price_sales_ratio
            if price_sales_ratio in ['N/A',"-"]  or float(price_sales_ratio) <= 0: 
                stock_new['price_sales_ratio_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['price_sales_ratio_log'] = {"original":price_sales_ratio, "converted":math.copysign(math.log(abs(float(price_sales_ratio)),10),float(price_sales_ratio))}
     
            #Earnings before Interest, Tax, Depreciation, and Amortization
            ebitda = stock['ebitda']
            ebitda = ebitda.replace(",","")
            if ebitda in ['N/A',"-","0"]: 
                stock_new['ebitda_log'] = {"original":'N/A', "converted":np.nan}
            elif ebitda[-1] == 'T': 
                stock_new['ebitda_log'] = {"original":ebitda, "converted":math.copysign(math.log(abs(float(float(ebitda[0:-1])))*1e12,10),float(ebitda[0:-1]))}
            elif ebitda[-1] == 'B':
                stock_new['ebitda_log'] = {"original":ebitda, "converted":math.copysign(math.log(abs(float(ebitda[0:-1]))*1e9,10),float(ebitda[0:-1]))}
            elif ebitda[-1] == 'M':
                stock_new['ebitda_log'] = {"original":ebitda, "converted":math.copysign(math.log(abs(float(ebitda[0:-1]))*1e6,10),float(ebitda[0:-1]))}
            elif ebitda[-1] == 'K': 
                stock_new['ebitda_log'] = {"original":ebitda, "converted":math.copysign(math.log(abs(float(ebitda[0:-1]))*1e3,10),float(ebitda[0:-1]))}       
            else:
                stock_new['ebitda_log'] = {"original":ebitda, "converted":math.copysign(math.log(abs(float(ebitda)),10),float(ebitda))}

            #dividend yield
            dividend_yield = stock['dividend_yield']
            del stock['dividend_yield']
            if dividend_yield in ['N/A',"-"]:
                stock_new['dividend_yield'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['dividend_yield'] = {"original":dividend_yield, "converted":float(dividend_yield)}
                
            #dividends per share
            dividend_per_share = stock['dividend_per_share']
            if dividend_per_share in ['N/A',"-"]:
                stock_new['dividend_per_share'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['dividend_per_share'] = {"original":dividend_per_share, "converted":float(dividend_per_share)}

            #Short ratio
            short_ratio = stock['short_ratio']
            if short_ratio in ['N/A',"-"]  or float(short_ratio) <= 0:
                stock_new['short_ratio_log'] = {"original":'N/A', "converted":np.nan}
            else:
                stock_new['short_ratio_log'] = {"original":short_ratio, "converted":math.log(abs(float(short_ratio)),10)}
            

            df_conv = pd.DataFrame([x["converted"] for x in stock_new.values()],index = stock_new.keys(),columns = [tick])
            df_orig = pd.DataFrame([x["original"] for x in stock_new.values()],index = stock_new.keys(),columns = [tick]) 
            return (df_orig,df_conv)
        
        #loop through all available stocks
        print("stock stats are being remapped to log formats")
        def convertAll():
            i = 0
            print("total stocks: " + str(len(stocksInfo.keys())))
            for tick in stocksInfo.keys():
                print("\r"+str(i)+ " stock is being processed"),
                i += 1
                if tick in stocksAll.index:
                    (orig, conv) = stockConvert(tick)
                    try:
                        df_orig = df_orig.join(orig)
                    except:
                        df_orig = pd.DataFrame(orig)
                    try:
                        df_conv = df_conv.join(conv)
                    except:
                        df_conv = pd.DataFrame(conv)                            
            return (df_orig, df_conv)
        
        def loadData():
            file = module_dir + "stocks_data.pickle"
            f = open(file,"rb") 
            data = pickle.load(f)
            file = module_dir + "stocks_data_orig.pickle"
            f = open(file,"rb") 
            data_orig = pickle.load(f)      
            return (data,data_orig) 
        
        if stockconvert_run == True or os.path.isfile(module_dir + 'stocks_data.pickle') == False:
            stocksDataOrig, stocksData = convertAll()
            print("stocksDat and stocksDataOrig were recalculated")
            #dump stock_data and stocks_data_orig to file
            f = open(module_dir + "stocks_data.pickle","wb")
            pickle.dump(stocksData,f)
            f = open(module_dir + "stocks_data_orig.pickle","wb")
            pickle.dump(stocksDataOrig,f)
            print("stocksDat and stocksDataOrig were pickled for faster access")
        else:
            stocksData, stocksDataOrig = loadData()
            print("stocksDat and stocksDataOrig were unpickled")
            
        print("\r");
        print("some stock stats were remapped to log formats")
        
        #remove stocksData columns which have less than 5 non-zero values
        stat = (stocksData.count(axis=0) > -1)# was 16, changed to -1 to include all of the stocks
        selStocks = stat[stat == True].index#]
        stocksDataSelected = stocksData[selStocks]
        print("select all stocks, even with all NANs")
        
        #remove stocksDataOrig columns where StocsData columns have less than 5 not zero values
        stat = (stocksData.count(axis=0) > -1) # was 16, changed to -1 to include all of the stocks
        selStocks = stat[stat == True].index#]
        stocksDataOrigSelected = stocksDataOrig[selStocks]
        print("select all stocks, even with all NANs")
     
        #data normalization
        n_baskets = 30
        stocksDataT = copy.deepcopy(stocksDataSelected.T)

        #normalizing employees_log (identified range [0,7])
        stocksDataT["employees_log"] = ((stocksDataT["employees_log"] -(0))/(7 - (0))*n_baskets)
        stocksDataT["employees_log"][stocksDataT["employees_log"] > n_baskets] = n_baskets
        stocksDataT["employees_log"][stocksDataT["employees_log"] < 0] = 0
        stocksDataT["employees_log"][np.isfinite(stocksDataT["employees_log"])] = \
        stocksDataT["employees_log"][np.isfinite(stocksDataT["employees_log"])].astype(int)
        
        #normalizing eps (identified range [-3,10]
        stocksDataT["eps"] = ((stocksDataT["eps"] -(-3))/(10 - (-3))*n_baskets)
        stocksDataT["eps"][stocksDataT["eps"] > n_baskets] = n_baskets
        stocksDataT["eps"][stocksDataT["eps"] < 0] = 0
        stocksDataT["eps"][np.isfinite(stocksDataT["eps"])] = \
        stocksDataT["eps"][np.isfinite(stocksDataT["eps"])].astype(int)

        #normalizing market_cap_log (identified range [2,13]
        stocksDataT["market_cap_log"] = ((stocksDataT["market_cap_log"] -2)/(13-2)*n_baskets)
        stocksDataT["market_cap_log"][stocksDataT["market_cap_log"] > n_baskets] = n_baskets
        stocksDataT["market_cap_log"][stocksDataT["market_cap_log"] < 0] = 0
        stocksDataT["market_cap_log"][np.isfinite(stocksDataT["market_cap_log"])] = \
        stocksDataT["market_cap_log"][np.isfinite(stocksDataT["market_cap_log"])].astype(int)

        #normalizing short_ratio_log (identified range [-2,3]
        stocksDataT["short_ratio_log"] = ((stocksDataT["short_ratio_log"] -(-2))/(3 - (-2))*n_baskets)
        stocksDataT["short_ratio_log"][stocksDataT["short_ratio_log"] > n_baskets] = n_baskets
        stocksDataT["short_ratio_log"][stocksDataT["short_ratio_log"] < 0] = 0
        stocksDataT["short_ratio_log"][np.isfinite(stocksDataT["short_ratio_log"])] = \
        stocksDataT["short_ratio_log"][np.isfinite(stocksDataT["short_ratio_log"])].astype(int)

        #normalizing dividend_yield (identified range [-2,55]
        stocksDataT["dividend_yield"] = ((stocksDataT["dividend_yield"] -(-2))/(55-(-2))*n_baskets)
        stocksDataT["dividend_yield"][stocksDataT["dividend_yield"] > n_baskets] = n_baskets
        stocksDataT["dividend_yield"][stocksDataT["dividend_yield"] < 0] = 0
        stocksDataT["dividend_yield"][np.isfinite(stocksDataT["dividend_yield"])] = \
        stocksDataT["dividend_yield"][np.isfinite(stocksDataT["dividend_yield"])].astype(int)

        #normalizing ebitda_log (identified range [-12,1]
        stocksDataT["ebitda_log"] = ((stocksDataT["ebitda_log"] - (-12))/(12- (-12))*n_baskets)
        stocksDataT["ebitda_log"][stocksDataT["ebitda_log"] > n_baskets] = n_baskets
        stocksDataT["ebitda_log"][stocksDataT["ebitda_log"] < 0] = 0
        stocksDataT["ebitda_log"][np.isfinite(stocksDataT["ebitda_log"])] = \
        stocksDataT["ebitda_log"][np.isfinite(stocksDataT["ebitda_log"])].astype(int)

        #normalizing price_sales_ratio_log (identified range [0,3]
        stocksDataT["price_sales_ratio_log"] = ((stocksDataT["price_sales_ratio_log"] -0)/(3-0)*n_baskets)
        stocksDataT["price_sales_ratio_log"][stocksDataT["price_sales_ratio_log"] > n_baskets] = n_baskets
        stocksDataT["price_sales_ratio_log"][stocksDataT["price_sales_ratio_log"] < 0] = 0
        stocksDataT["price_sales_ratio_log"][np.isfinite(stocksDataT["price_sales_ratio_log"])] = \
        stocksDataT["price_sales_ratio_log"][np.isfinite(stocksDataT["price_sales_ratio_log"])].astype(int)

        #normalizing peg_log (identified range [0,2.5]
        stocksDataT["peg_log"] = ((stocksDataT["peg_log"] -0)/(2.5-0)*n_baskets)
        stocksDataT["peg_log"][stocksDataT["peg_log"] > n_baskets] = n_baskets
        stocksDataT["peg_log"][stocksDataT["peg_log"] < 0] = 0
        stocksDataT["peg_log"][np.isfinite(stocksDataT["peg_log"])] = \
        stocksDataT["peg_log"][np.isfinite(stocksDataT["peg_log"])].astype(int)

        #normalizing pe_log (identified range [0,4]
        stocksDataT["pe_log"] = ((stocksDataT["pe_log"] -0)/(4-0)*n_baskets)
        stocksDataT["pe_log"][stocksDataT["pe_log"] > n_baskets] = n_baskets
        stocksDataT["pe_log"][stocksDataT["pe_log"] < 0] = 0
        stocksDataT["pe_log"][np.isfinite(stocksDataT["pe_log"])] = \
        stocksDataT["pe_log"][np.isfinite(stocksDataT["pe_log"])].astype(int)

        #normalizing book_value (identified range [-10,50]
        stocksDataT["book_value"] = ((stocksDataT["book_value"] -(-10))/(50 - (-10))*n_baskets)
        stocksDataT["book_value"][stocksDataT["book_value"] > n_baskets] = n_baskets
        stocksDataT["book_value"][stocksDataT["book_value"] < 0] = 0
        stocksDataT["book_value"][np.isfinite(stocksDataT["book_value"])] = \
        stocksDataT["book_value"][np.isfinite(stocksDataT["book_value"])].astype(int)

        #normalizing fifty_two_week_low_log (identified range [0,3]
        stocksDataT["fifty_two_week_low_log"] = ((stocksDataT["fifty_two_week_low_log"] -0)/(3-0)*n_baskets)
        stocksDataT["fifty_two_week_low_log"][stocksDataT["fifty_two_week_low_log"] > n_baskets] = n_baskets
        stocksDataT["fifty_two_week_low_log"][stocksDataT["fifty_two_week_low_log"] < 0] = 0
        stocksDataT["fifty_two_week_low_log"][np.isfinite(stocksDataT["fifty_two_week_low_log"])] = \
        stocksDataT["fifty_two_week_low_log"][np.isfinite(stocksDataT["fifty_two_week_low_log"])].astype(int)

        #normalizing last_trade_price_log (identified range [0,3]
        stocksDataT["last_trade_price_log"] = ((stocksDataT["last_trade_price_log"] -0)/(3-0)*n_baskets)
        stocksDataT["last_trade_price_log"][stocksDataT["last_trade_price_log"] > n_baskets] = n_baskets
        stocksDataT["last_trade_price_log"][stocksDataT["last_trade_price_log"] < 0] = 0
        stocksDataT["last_trade_price_log"][np.isfinite(stocksDataT["last_trade_price_log"])] = \
        stocksDataT["last_trade_price_log"][np.isfinite(stocksDataT["last_trade_price_log"])].astype(int)

        #normalizing average_daily_volume_log (identified range [1,9]
        stocksDataT["average_daily_volume_log"] = ((stocksDataT["average_daily_volume_log"] -1)/(9-1)*n_baskets)
        stocksDataT["average_daily_volume_log"][stocksDataT["average_daily_volume_log"] > n_baskets] = n_baskets
        stocksDataT["average_daily_volume_log"][stocksDataT["average_daily_volume_log"] < 0] = 0
        stocksDataT["average_daily_volume_log"][np.isfinite(stocksDataT["average_daily_volume_log"])] = \
        stocksDataT["average_daily_volume_log"][np.isfinite(stocksDataT["average_daily_volume_log"])].astype(int)

        #normalizing percent_change (identified range [-5,5]
        stocksDataT["percent_change_sqrt"] = ((stocksDataT["percent_change_sqrt"] -(-5))/(5 - (-5))*n_baskets)
        stocksDataT["percent_change_sqrt"][stocksDataT["percent_change_sqrt"] > n_baskets] = n_baskets
        stocksDataT["percent_change_sqrt"][stocksDataT["percent_change_sqrt"] < 0] = 0
        stocksDataT["percent_change_sqrt"][np.isfinite(stocksDataT["percent_change_sqrt"])] = \
        stocksDataT["percent_change_sqrt"][np.isfinite(stocksDataT["percent_change_sqrt"])].astype(int)
        
        #normalizing dividend_per_share (identified range [0,7]
        stocksDataT["dividend_per_share"] = ((stocksDataT["dividend_per_share"] -0)/(7 - 0)*n_baskets)
        stocksDataT["dividend_per_share"][stocksDataT["dividend_per_share"] > n_baskets] = n_baskets
        stocksDataT["dividend_per_share"][stocksDataT["dividend_per_share"] < 0] = 0
        stocksDataT["dividend_per_share"][np.isfinite(stocksDataT["dividend_per_share"])] = \
        stocksDataT["dividend_per_share"][np.isfinite(stocksDataT["dividend_per_share"])].astype(int)

        #normalizing two_hundred_sma_log (identified range [0,3]
        stocksDataT["two_hundred_sma_log"] = ((stocksDataT["two_hundred_sma_log"] -0)/(3-0)*n_baskets)
        stocksDataT["two_hundred_sma_log"][stocksDataT["two_hundred_sma_log"] > n_baskets] = n_baskets
        stocksDataT["two_hundred_sma_log"][stocksDataT["two_hundred_sma_log"] < 0] = 0
        stocksDataT["two_hundred_sma_log"][np.isfinite(stocksDataT["two_hundred_sma_log"])] = \
        stocksDataT["two_hundred_sma_log"][np.isfinite(stocksDataT["two_hundred_sma_log"])].astype(int)

        #normalizing fifty_two_week_high_log (identified range [0,3]
        stocksDataT["fifty_two_week_high_log"] = ((stocksDataT["fifty_two_week_high_log"] -0)/(3-0)*n_baskets)
        stocksDataT["fifty_two_week_high_log"][stocksDataT["fifty_two_week_high_log"] > n_baskets] = n_baskets
        stocksDataT["fifty_two_week_high_log"][stocksDataT["fifty_two_week_high_log"] < 0] = 0
        stocksDataT["fifty_two_week_high_log"][np.isfinite(stocksDataT["fifty_two_week_high_log"])] = \
        stocksDataT["fifty_two_week_high_log"][np.isfinite(stocksDataT["fifty_two_week_high_log"])].astype(int)

        #normalizing fifty_sma_log (identified range [0,3]
        stocksDataT["fifty_sma_log"] = ((stocksDataT["fifty_sma_log"] -0)/(3-0)*n_baskets)
        stocksDataT["fifty_sma_log"][stocksDataT["fifty_sma_log"] > n_baskets] = n_baskets
        stocksDataT["fifty_sma_log"][stocksDataT["fifty_sma_log"] < 0] = 0
        stocksDataT["fifty_sma_log"][np.isfinite(stocksDataT["fifty_sma_log"])] = \
        stocksDataT["fifty_sma_log"][np.isfinite(stocksDataT["fifty_sma_log"])].astype(int)

        #normalizing price_book_ratio_log (identified range [-1,3]
        stocksDataT["price_book_ratio_log"] = ((stocksDataT["price_book_ratio_log"] -(-1))/(3 - (-1))*n_baskets)
        stocksDataT["price_book_ratio_log"][stocksDataT["price_book_ratio_log"] > n_baskets] = n_baskets
        stocksDataT["price_book_ratio_log"][stocksDataT["price_book_ratio_log"] < 0] = 0
        stocksDataT["price_book_ratio_log"][np.isfinite(stocksDataT["price_book_ratio_log"])] = \
        stocksDataT["price_book_ratio_log"][np.isfinite(stocksDataT["price_book_ratio_log"])].astype(int)

        #normalizing volume_log (identified range [1,9]
        stocksDataT["volume_log"] = ((stocksDataT["volume_log"] -1)/(9-1)*n_baskets)
        stocksDataT["volume_log"][stocksDataT["volume_log"] > n_baskets] = n_baskets
        stocksDataT["volume_log"][stocksDataT["volume_log"] < 0] = 0
        stocksDataT["volume_log"][np.isfinite(stocksDataT["volume_log"])] = \
        stocksDataT["volume_log"][np.isfinite(stocksDataT["volume_log"])].astype(int)

        stocksDataNorm = stocksDataT.T
        #reorder parameters
        #order = [7,5,11,15,21,1,10,14,9,18,4,19,0,20,2,12,3,13,16,6,17,8]
        #stocksDataNorm = stocksDataNorm.iloc[order,:]
        #stocksDataOrig2 = stocksDataOrigSelected.iloc[order,:]
        #print("stocks stats were reordered in more convenient way")
        print("stocks stats were renormalized and remapped to the 1-30 range")

        #sort stocks in popularity order
        #popularity is defined as normalized market Capitalization + Volume + last_trade_price
        #get list of stocks ordered in popularity
        stocksDataNormPop = pd.DataFrame(stocksDataNorm.T["market_cap_log"]+stocksDataNorm.T["volume_log"]+stocksDataNorm.T["last_trade_price_log"],columns=["Pop"])
        stocksDataNormPop = stocksDataNormPop.fillna(0)
        stocksDataNormPopSorted = stocksDataNormPop.sort(["Pop"], ascending = 0)
        print("stocks sorted in popularity order")
     
        #parameters tittles
        titles = ["Business summary","Sector","Industry","Stock exchange", "Country","Index memberships", "Full time employees","Last traded price, USD", "Price change since last market close, %","Market capitalization, USD","Volume","Average daily volume",
            "Fifty day moving average, USD","Two hundred day moving average, USD","Fifty two week high, USD","Fifty two week low, USD","Earnings per share(EPS), USD",
            "Price earnings ratio (P/E)", "Price earnings growth ratio (PEG)", "Book value, USD", "Price book value ratio","Price sales ratio","EBITDA, USD",
            "Dividend yield", "Dividend per share (DPS), USD", "Short ratio"]


        #save stocksDataNorm
        stocks_data_norm = {}
        stocks = stocksDataNormPopSorted.index.values.tolist()
        stocks_data_norm['stocks'] = [stock.encode() for stock in stocks]
        stocks_data_norm['parameters'] = titles
        stocks_data_norm['names'] = []
        stocks_data_norm['time_stamp'] = time_stamp
        stocks_data_norm['data']={}
        stocks_data_norm['labels']={}
        for stock in stocks:
            col = []
            for i in range(len(stocksDataNorm[stock].values)):
                x = stocksDataNorm[stock].values.tolist()[i]
                if np.isnan(x) or x == 'nan':
                    col.append(None)
                else:
                    #converting back indices
                    if titles[i+1] == "Index memberships": # i-> i+1  to include business summary into index
                        col.append(" ".join(list(str(int(x)))))
                    else:
                        col.append(x)
            stocks_data_norm['data'][stock.encode()] = col
            stocks_data_norm['labels'][stock.encode()] = [str(x) for x in stocksDataOrigSelected[stock].values.tolist()]
            stocks_data_norm['names'].append(str(stocksAll.loc[stock]["Name"]).encode())  
        #dump stocks_data_norm to file
        f = open(module_dir + "stocks_data_norm.json","w")
        json.dump(stocks_data_norm,f)
        f.close()
        #stocksDataNorm.columns.values.tolist()
        print("normalized stocks were saved to stocks_data_norm.json file")
        
		# copy stocks_data_norm.json
        data_file = data_dir + "stock_data_norm_"+day+".json"
        shutil.copyfile(module_dir + "stocks_data_norm.json",data_file)
        print("stocks_data_norm.json file was copied to "+data_file)
        
        #push stocks_data_norm to couchDB
        class Book(couch.Document):
            title = ""
            data = {}
            
        def couchDBPush(stocks_data_norm,summary_tfidf):
            #updating stockrec
            user = "akedulderideverthespecom"
            password = "yHVHwdcwa7crwnRl0nLd2Fqf"
            CouchDBAuth = couch.resource.CouchdbResource(filters=[BasicAuth(user, password)])
            uri = "https://bespam.cloudant.com"
            CouchDBServer = couch.Server(uri, resource_instance=CouchDBAuth)
            database = "stockrec"
            db = CouchDBServer.get_db(database)
            title = "stocks"
            #saving stocks_data_norm
            if not db.doc_exist(title):
                doc = Book(data = stocks_data_norm, summary_tfidf = summary_tfidf,_id = title,title=title,time=time.time())   
            else:
                doc = db.get(title)
                doc["data"] = stocks_data_norm
                doc["summary_tfidf"] = summary_tfidf
                doc["title"] = title
                doc["time"]= time.time()
            db.save_doc(doc)         
            
            #updating stockrec_userdata
            database = "stockrec_userdata"
            db = CouchDBServer.get_db(database)
            new_parameters = stocks_data_norm['parameters']
            old_parameters = db.get("parameters")["names"]
            parameters_change = (sum([a == b for a,b in zip(new_parameters, old_parameters)]) != len(new_parameters))
            # if parameters names or configuration were change (update all user profiles)
            if parameters_change:
                for document in db:
                    doc = db.get(document["id"])
                    profiles = doc["profiles"]
                    if document["id"] == "parameters":
                        doc["names"] = new_parameters
                    for name in profiles.keys():
                        new_profile = []
                        for param in new_parameters:
                            new_profile.append(1 if param not in old_parameters \
                            else profiles[name][old_parameters.index(param)])
                        profiles[name] = new_profile
                    doc["profiles"] = profiles
                    db.save_doc(doc)
                    
                
            
        if couchDB_push == True:
			couchDBPush(stocks_data_norm, summary_tfidf)
			print("normalized stocks data was pushed to the couchDB")
    except:
        pdb.set_trace()
        raise
    
if __name__ == '__main__':       
    analyze(False, False, True)        