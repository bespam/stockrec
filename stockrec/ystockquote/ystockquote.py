#Modified and improved version of https://github.com/cgoldberg/ystockquote

from urllib2 import Request, urlopen
from urllib import urlencode
import pdb
import copy
        

def get_all(symbols):
    """
    Get all available quote data for the given ticker symbol.

    Returns a dictionary.
    """
    
    # since some info contains commas, we will separate names by a stock symbol (s).
    ids = ['y','d','b2', 'r1','b3','q','p','o','c1','d1','c','d2','c6','t1','k2','p2','c8','m5','c3','m6','g','m7','h','m8','k1','m3','l','m4','l1','t8','w1','g1','w4','g3','p1',
        'g4','m','g5','m2','g6','k','i','j','j1','j5','j3','k4','j6','n','k5','n4','w','s1','x','j2','v','a5','b6','k3','t7','a2','t6','i5','l2','e','l3','e7','v1',
        'e8','v7','e9','s6','b4','j4','p5','p6','r','r2','r5','r6','r7','s7','f6']
    
    stats = "s".join(ids)
    symbols_plus = "+".join(symbols)
    resp = "initialized"
    symbol = "initialized"
    req = "initialized"
    try:
        #requesting data
        url = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (symbols_plus, stats)
        req = Request(url)
        resp = urlopen(req)
        resp_copy = copy.copy(resp)
        stats = {}
        for symbol in symbols:
            line = resp.readline()
            content = line.decode().strip()
            values = content.split(',"'+symbol+'",')
            if len(values) != 83:
                return {"symbols":symbols, "error":"error", "req":req, "resp":resp_copy, "symbol":symbol}
            stats[symbol] = dict(
            dividend_yield=values[0], #"y"
            dividend_per_share=values[1], #"d"
            ask_realtime=values[2], #"b2"
            dividend_pay_date=values[3], #"r1"
            bid_realtime=values[4], #"b3"
            ex_dividend_date=values[5], #"q"
            previous_close=values[6], #"p"
            today_open=values[7], #"o"
            change=values[8], #"c1"
            last_trade_date=values[9], #"d1"
            change_percent_change=values[10], #"c"
            trade_date=values[11],#"d2"
            change_realtime=values[12], #"c6"
            last_trade_time=values[13], #"t1"
            change_percent_realtime=values[14], #"k2"
            change_percent=values[15], #"p2"
            after_hours_change_realtime=values[16], #"c8"
            change_200_sma=values[17], #"m5"
            comission = values[18], #"c3"
            percent_change_200_sma=values[19], #"m6"
            todays_low=values[20], #"c3"
            change_50_sma=values[21],#"m7"
            todays_high=values[22],#"h"
            percent_change_50_sma=values[23], #"m8"
            last_trade_realtime_time=values[24], #"k1"
            fifty_sma=values[25], #"m3"
            last_trade_time_plus=values[26], #"l"
            twohundred_sma=values[27], #"m4"
            last_trade_price=values[28], #"l1" 
            one_year_target=values[29], #"t8"
            todays_value_change=values[30], #"w1"
            holdings_gain_percent=values[31],#"g1"
            todays_value_change_realtime=values[32], #"w4"
            annualized_gain=values[33], #"g3"
            price_paid=values[34], #"p1"
            holdings_gain=values[35], #"g4"
            todays_range=values[36], #"m"
            holdings_gain_percent_realtime=values[37], #"g5"
            todays_range_realtime=values[38], #"m2"
            holdings_gain_realtime=values[39], #"g6"
            fiftytwo_week_high=values[40], #"k"
            more_info=values[41], #"i"
            fiftytwo_week_low=values[42], #"j"
            market_cap=values[43], #"j1"
            change_from_52_week_low=values[44], #"j5"
            market_cap_realtime=values[45], #"j3"
            change_from_52_week_high=values[46], #"k4"
            percent_change_from_52_week_low=values[47],#"j6"
            company_name=values[48], #"n"
            percent_change_from_52_week_high=values[49], #"k5"
            notes=values[50], #"n4"
            fiftytwo_week_range=values[51], #"w"
            shares_owned=values[52], #"s1"
            stock_exchange=values[53], #"x"
            shares_outstanding=values[54], #"j2"
            volume=values[55], #"v"
            ask_size=values[56], #"a5"
            bid_size=values[57], #"b6"
            last_trade_size=values[58], #"k3"
            ticker_trend=values[59], #"t7"
            average_daily_volume=values[60], #"a2"
            trade_links=values[61], #"t6"
            order_book_realtime=values[62], #"i5"
            high_limit=values[63],#"l2"
            eps=values[64], #"e"
            low_limit=values[65], #"l3"
            eps_estimate_current_year=values[66], #"e7"
            holdings_value=values[67], #"v1"
            eps_estimate_next_year=values[68], #"e8"
            holdings_value_realtime=values[69], #"v7"
            eps_estimate_next_quarter=values[70], #"e9"
            revenue=values[71], #"s6"
            book_value=values[72], #"b4"
            ebitda=values[73], #"j4"
            price_sales=values[74], #"p5"
            price_book=values[75], #"p6"
            pe=values[76], #"r"
            pe_realtime=values[77], #"r2"
            peg=values[78], #"r5"
            price_eps_estimate_current_year=values[79], #"r6"
            price_eps_estimate_next_year=values[80], #"r7"
            short_ratio=values[81], #"s7"
            float_shares=values[82]) #"f6"
        return stats
    except Exception, e:
        return {"symbols":symbols, "error":e, "req":req, "resp":resp, "symbol":symbol}

if __name__ == '__main__':       
    print get_all(["GOOG","AAPL"])   