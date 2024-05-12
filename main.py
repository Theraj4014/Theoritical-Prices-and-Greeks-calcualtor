import option_strategy as opt
from constants import Keys
from datetime import date
from model import StrikeEntry

if __name__ == '__main__':
     # TODO: Begin from here
     # TODO: 1. Define some info
     symbol = 'nifty'
     # symbol = 'banknifty'
     expiry_month = 1
     expiry_year = 2019
     strike_data = [StrikeEntry(10800, Keys.call, Keys.sell, 276.7),
                    StrikeEntry(11000, Keys.call, Keys.buy, 154.15),
     ]

     spot_range = [10000, 12000]

     # TODO: 2. Option Strategy Analysis
     opt.options_strategy(symbol, strike_data, expiry_month, expiry_year, date(2019, 1, 1), spot_range=spot_range)