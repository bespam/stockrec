# function which accepts the list of stock names,
# downloads the profile page for each of the stock, 
# and extracts stock parameters and company description


from urllib import urlopen
from bs4 import BeautifulSoup
import sys
import pdb
import time

# run every first Sunday of the each Month 
def getProfiles(stocks):
    profiles ={}
    print("Downloading profiles, number of stocks: "+str(len(stocks)))
    i = 0
    for stock in stocks:
        print("Downloading "+str(i) +": " +stock)+"                          \r",
        i += 1
        if i > 0:
            profile = getProfile(stock)
            profiles[stock] = profile
            #time.sleep(3)
    return profiles

# run for each stock
def getProfile(stock):
    #default values
    address = "N/A"
    country = "N/A"
    indexMemberships = ["N/A"]
    fullTimeEmployees = "N/A"
    businessSummary = "N/A"
    
    # load webpage
    request = "http://finance.yahoo.com/q/pr?s="+stock+"&ql=1"
    response = urlopen(request)
    html = response.read().decode('utf-8')
    #make soup
    soup = BeautifulSoup(html)
    #get main container
    baseContainer = soup.find("table",{"id": "yfncsumtab"})
    if baseContainer == None:
        profile = {"address":"N/A","country":"N/A","indexMemberships":["N/A"],"fullTimeEmployees":"N/A","businessSummary":"N/A"}
        return profile 
    mainContainer = baseContainer.findAll('td')[3]
    #check if profile exists
    if "no Profile" in mainContainer.next.text:
        profile = {"address":"N/A","country":"N/A","indexMemberships":["N/A"],"fullTimeEmployees":"N/A","businessSummary":"N/A"}
        return profile 
    #get Profile
    profileContainer = mainContainer.findAll('br')
    address = []
    for row in profileContainer:
        if row.contents[0].string == None:
            address = "N/A"
            break
        if len(row.contents) > 1 and row.contents[1].string == 'Map':
            str = row.contents[0].string[:-3].encode("ascii","xmlcharrefreplace")
            address.append(str)
            country = str
            break
        address.append(row.contents[0].string.encode("ascii","xmlcharrefreplace"))
    #remove extra symbols
    address = " ".join(address).replace(',',"").replace('\n',"").replace('\t',"")
    #clean extra whitespaces
    address = " ".join(address.split())
    
    #getBusinessDetails
    detailsContainer = mainContainer.find(text="Details")
    if detailsContainer != None:
        detailsContainer = detailsContainer.parent.parent.parent.parent.parent
        #get Index Membership field
        indexContainer = detailsContainer.find(text="Index Membership:").next.findAll("a")
        if indexContainer != None:
            for indexM in indexContainer:
                indexMemberships.append(indexM.string.encode("ascii","xmlcharrefreplace"))
            #get FullTimeEmployees field
            fullTimeEmployees = detailsContainer.find(text="Full Time Employees:").next.string.encode("ascii","xmlcharrefreplace").replace(",","")
        if len(indexMemberships) > 1: indexMemberships = indexMemberships[1:]
        
        #getBusinessSummary
        summaryContainer = mainContainer.find(text="Business Summary")
        if summaryContainer != None:
            businessSummary = summaryContainer.next.next.next.string
            if businessSummary == None:
                businessSummary = "N/A"
            else:
                businessSummary = businessSummary.encode("ascii","xmlcharrefreplace")
            
    profile = {"address":address,"country":country,"indexMemberships":indexMemberships,"fullTimeEmployees":fullTimeEmployees,"businessSummary":businessSummary}
    return profile
    
    
    
    
if __name__ == '__main__':       
    if len(sys.argv) > 1:
        stock = sys.argv[1]
    else:
        stock = "NOK"
    print getProfile(stock)
    