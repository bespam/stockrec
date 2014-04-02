# -------------------------------------------------------------------------------------
# Stock similarity recommender engine based on the stock statistical and profile data
# -------------------------------------------------------------------------------------

import os
import re
import pdb
import json
import time
import urllib2
import shutil
import sys
import socket
import git
import datetime
from stockrec import stockrec
from ystockdownloader import ystockdownloader
from ystockprofile import ystockprofile
from summarytfidf import summarytfidf
import csv
import shutil
import pandas as pd


#A daemon code to be run 24/7. Purpose: to run second level python scripts at a appropriate timing.  
def run_daemon():
    #directory of the current module
    module_dir = os.path.dirname(__file__)
    if module_dir != "": module_dir = module_dir + "\\"
    
    #starting daemon
    print "-------------------------------------------------------------------------------"
    print " "
    print " "  
    # running daemon in infinite loop
    while True:
        try: 
            hour = int(time.strftime('%H'))
            minute = int(time.strftime('%M'))
            day = int(time.strftime('%w')) # week day (0-Sunday)
            # roll back 7 hours to have the late analysis to be saved at the previous day
            date_label = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours=7),'%Y-%m-%d')
            month_label = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours=7),'%Y-%m')
            dataDir = module_dir +"data\\"
            data_file = dataDir + "stock_data_norm_"+date_label+".json"
            stocks_info_file = dataDir + "stocks_info.csv"
            stocks_profiles_file = dataDir + "stocks_profiles.json"
            summary_tfidf_file = dataDir + "summary_tfidf.json"
            # main recommender analysis
            # the stock data are analysed on a daily basis right after US markets are closed.
            if day in [1,2,3,4,5]:
                if hour >= 18: # 18 default
                    #check if stock_data_norm.json needs to be updated
                    if os.path.isfile(data_file):
                        print "Data file exists, sleeping one hour. ", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",
                    else:
                        stockrec.analyze(True, True, True);
                        print "Data was recalculated, sleeping one hour. ", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time()))          
                else:
                    print "Waiting until 18:00 CDT for Stock Markets to close, sleeping one hour.", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",
            # during the weekends, where other processes are finished, 
            # check whether the list of US stocks needs to be updated.
            # new list is pulled from the Yahoo lookup website every month.
            if day == 6 or day == 0:
                #check if stocks.csv needs to be updated 
                if os.path.isfile(stocks_info_file):
                    time_check = time.strftime("%Y-%m",time.localtime(os.path.getmtime(stocks_info_file))) != month_label
                else:
                    time_check = True
                if time_check:                
                    print "\n ystockdownloader is being run"
                    ystockdownloader.main()
                    print "ystockdownloader has successfully finished"
                    shutil.copyfile("ystockdownloader\\stocks_info.csv",stocks_info_file)
                    print "stocks_info.csv file was copied to "+stocks_info_file
                else:
                    print "stocks_info.csv file exists, sleeping one hour. ", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",                    
            # On Sun, where other processes are finished, 
            # check whether the stock profiles needs to be updated
            # profiles for each stock in the list are pulled from the Yahoo profiles website every month.
            if day == 0:
                #downloading stock profiles if needed
                if os.path.isfile(stocks_profiles_file):
                    time_check = time.strftime("%Y-%m",time.localtime(os.path.getmtime(stocks_profiles_file))) != month_label
                else:
                    time_check = True
                if time_check:
                    print "\n ystockprofiles is being run"
                    stocks = pd.read_csv(stocks_info_file).Ticker.values.tolist()
                    stocksProfiles = ystockprofile.getProfiles(stocks)
                    #dump stocksProfiles to file
                    f = open("ystockprofile\\stocks_profiles.json","w")
                    json.dump(stocksProfiles,f)
                    f.close()
                    print "Stocks Profiles were downloaded and saves into stocks_profles.json" 
                    shutil.copyfile("ystockprofile\\stocks_profiles.json",stocks_profiles_file)
                    print "stocks_profiles.json file was copied to "+stocks_profiles_file                
                    print "Waiting, sleeping one hour.", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",
                else:
                    print "Stocks_profiles.json file exists, sleeping one hour. ", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",
                #once stock profiles are reloaded, profiles similarity should be recalculated
                #check if summary_tfidf file exists or a old one
                if os.path.isfile(summary_tfidf_file):
                    time_check = time.strftime("%Y-%m",time.localtime(os.path.getmtime(summary_tfidf_file))) != month_label
                else:
                    time_check = True
                if time_check:
                    print "\n summarytfidf is being run"
                    # reading profile file
                    f = open(stocks_profiles_file,"r") 
                    profile = json.load(f)
                    stocks= profile.keys()
                    summarytfidf.analysis(stocks,False,20,20,"")
                    print "Profile summaries were analysed and saved into summary_tfidf.json" 
                    shutil.copyfile("summarytfidf\\summary_tfidf.json",summary_tfidf_file)
                    print "summary_tfidf.json file was copied to "+summary_tfidf_file  
                    print "Waiting, sleeping one hour.", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",              
                else:
                    print "summary_tfidf.json file exists, sleeping one hour. ", "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time())),"\r",

            time.sleep(3600)
        except KeyboardInterrupt as ex:
            raise
        except:
            print "\nUnexpected error:", sys.exc_info()[0], "Time: ", time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(time.time()))
            raise
            time.sleep(60)
            #restart

            
if __name__ == '__main__':
    run_daemon()
  