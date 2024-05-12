import csv,os,datetime
from QuantLib import *
from datetime import date
import pandas as pd
import time
import numpy as np
from sqlalchemy import create_engine
from multiprocessing import Pool

start = time.time()
fo_path = 'D:/bhavcopy/fo/extracted'
cash_path ='D:/bhavcopy/cm/extracted'
index_path = 'D:/bhavcopy/indices/'
final_path = 'D:/bhavcopy/modified_csv/'

def get_greeks(spot,STRIKE_PR,EXPIRY_DT,SETTLE_PR,TIMESTAMP,OPTION_TYP):
    """
    It is used to find option greeks for the given inputs.
    Results are based on the implied volatility found using option price.
    :param spot_price: float
            (S) Spot price
    :param strike_price: float
            (K) Strike price
    :param expiry_date: date
            (T) Time to maturity i.e. expiry date
    :param option_type: str
            Type of option. Possible values: CE, PE
    :param option_price: float
            Price of the option
    :param calculation_date: date
            Observation date. If None is given then present date is taken.
    :param volatility: float
            Annualised Volatility for underlying.
    :return: tuple(float, float, float, float, float)
            Returns implied volatility, theta, gamma, delta, vega for the option data entered
    """
    maturity_date = Date(EXPIRY_DT.day, EXPIRY_DT.month, EXPIRY_DT.year)
    volatility = 0.13
    #option = Option.Call
    if OPTION_TYP == "CE":
        option = Option.Call
    if OPTION_TYP == "PE":
        option = Option.Put
    risk_free_rate = 0.1
    day_count = Actual365Fixed()
    calendar = India()
    calculation_date = Date(TIMESTAMP.day, TIMESTAMP.month, TIMESTAMP.year)
    Settings.instance().evaluationDate = calculation_date
   # construct the European Option
    payoff = PlainVanillaPayoff(option, STRIKE_PR)
    exercise = EuropeanExercise(maturity_date)
    #european_option = EuropeanOption(payoff, exercise)
    european_option = VanillaOption(payoff, exercise)

#Black-Scholes-Merton process
    spot_handle = QuoteHandle(SimpleQuote(spot))
    flat_ts = YieldTermStructureHandle(FlatForward(calculation_date, risk_free_rate, day_count))
    dividend_yield = YieldTermStructureHandle(FlatForward(calculation_date, 0, day_count))
    flat_vol_ts = BlackVolTermStructureHandle(BlackConstantVol(calculation_date, calendar, volatility, day_count))
    bs_process = BlackScholesMertonProcess(spot_handle,dividend_yield, flat_ts, flat_vol_ts)
#option Greeks
    try:
        iv = european_option.impliedVolatility(SETTLE_PR,bs_process)
        #iv = iv * 100
        flat_vol_ts = BlackVolTermStructureHandle(BlackConstantVol(calculation_date, calendar, iv, day_count))
        bs_process = BlackScholesMertonProcess(spot_handle, dividend_yield, flat_ts, flat_vol_ts)

        european_option.setPricingEngine(AnalyticEuropeanEngine(bs_process))
        bs_price = european_option.NPV()
        iv = iv * 100
        Theta = european_option.thetaPerDay()
        gamma = european_option.gamma()
        delta = european_option.delta()
        vega = european_option.vega()/100
        rho = european_option.rho()

    except RuntimeError:
        iv = 0.0
        Theta = 0.0
        gamma = 0.0
        delta = 0.0
        vega = 0.0
        rho = 0.0




    return [iv,Theta,gamma,delta,vega,rho]

fo_obj = os.listdir(fo_path)
cash_obj = os.listdir(cash_path)
index_obj = os.listdir(index_path)

for (fo_file,cash_file,index_file) in zip(fo_obj,cash_obj,index_obj):
    start = time.time()
    # if int(fo_file[0:2])>10:
    fno_file = 'D:/bhavcopy/fo/extracted/' + fo_file
    spot_file = 'D:/bhavcopy/cm/extracted/' + cash_file
    ind_file = 'D:/bhavcopy/indices/' + index_file

    fo_data = pd.read_csv(fno_file)
    #fo_data = fo_data.loc[:, ~fo_data.Columns.Str.contains('Unnamed:15')]
    fo_data = fo_data.drop(columns=['Unnamed: 15'], errors='ignore')

    cash_data = pd.read_csv(spot_file)
    to_drop = ['SM','BZ','BE','N2','N3','N6','N8','NC','NF','NL','NN','NP','N4','N5','N7','W2','N9','NB','ND','NE','NA','N1',
               'NH','IV','NJ','NK','NO','P2','MF','NG','NI','GB','NQ','NU','NW','NZ','Y2','Y3','YA','Y9','YB','DR','Q1','P1']
    cash_data = cash_data[~cash_data['SERIES'].isin(to_drop)]
    cash_dict = dict(zip(cash_data['SYMBOL'],cash_data['CLOSE']))

    index_data = pd.read_csv(ind_file)
    to_drop = ['Nifty Next 50','Nifty 100','Nifty 200','Nifty 500','Nifty Free Float Midcap 100','Nifty Free Float Smallcap 100','Nifty50 Dividend Points','Nifty Auto','Nifty Energy','Nifty Financial Services','Nifty FMCG','Nifty Media','Nifty Metal','Nifty MNC','Nifty Pharma','Nifty PSU Bank','Nifty Realty','Nifty India Consumption','Nifty Commodities','Nifty Dividend Opportunities 50','Nifty Services Sector','Nifty50 Shariah','Nifty500 Shariah','Nifty Low Volatility 50','Nifty Alpha 50','Nifty High Beta 50','Nifty50 USD','Nifty100 Equal Weight','Nifty100 Liquid 15','Nifty50 Value 20','Nifty Midcap Liquid 15','Nifty Shariah 25','India VIX','Nifty Growth Sectors 15','Nifty50 TR 1x Inverse','Nifty50 TR 2x Leverage','Nifty50 PR 1x Inverse','Nifty50 PR 2x Leverage','Nifty Quality 30','NIFTY SME EMERGE','Nifty 50 Arbitrage','NIFTY50 Equal Weight','NIFTY Alpha Quality Value Low-Volatility 30','Nifty Private Bank','NIFTY LargeMidcap 250','Nifty Mahindra Group','Nifty Full Smallcap 100','Nifty Smallcap 250','Nifty MidSmallcap 400','Nifty Tata Group 25% Cap','Nifty Tata Group','Nifty100 Low Volatility 30','Nifty Midcap 150','NIFTY Alpha Quality Low-Volatility 30','NIFTY Quality Low-Volatility 30','NIFTY Alpha Low-Volatility 30','Nifty 50 Futures TR Index','Nifty 50 Futures Index','Nifty Full Midcap 100','Nifty Smallcap 50','Nifty Aditya Birla Group','Nifty 8-13 yr G-Sec','Nifty 4-8 yr G-Sec Index','Nifty 11-15 yr G-Sec Index','Nifty 15 yr and above G-Sec Index','Nifty Composite G-sec Index','Nifty 10 yr Benchmark G-Sec','Nifty 10 yr Benchmark G-Sec (Clean Price)','Nifty 1D Rate Index']
    index_data = index_data[~index_data['Index Name'].isin(to_drop)]

    index_data.rename(columns={'Index Date':'TIMESTAMP','Closing Index Value':'CLOSE'},inplace = True)

    index_data['SYMBOL'] = index_data['Index Name']
    index_data.set_index('Index Name',inplace=True)
    index_data.at['Nifty 50','SYMBOL'] = 'NIFTY'
    index_data.at['Nifty Bank','SYMBOL'] = 'BANKNIFTY'
    index_data.at['Nifty Infrastructure','SYMBOL'] = 'NIFTYINFRA'
    index_data.at['Nifty Midcap 50','SYMBOL'] = 'NIFTYMID50'
    index_data.at['Nifty IT','SYMBOL'] = 'NIFTYIT'
    index_data.at['Nifty PSE','SYMBOL'] = 'NIFTYPSE'
    index_data.at['Nifty CPSE','SYMBOL'] = 'NIFTYCPSE'

    index_dict = dict(zip(index_data['SYMBOL'],index_data['CLOSE']))

    cash_dict.update(index_dict)

    fo_data['EXPIRY_DT'] = fo_data['EXPIRY_DT'].apply(lambda  x : datetime.datetime.strptime(x, "%d-%b-%Y"))
    fo_data['TIMESTAMP'] = fo_data['TIMESTAMP'].apply(lambda  x : datetime.datetime.strptime(x, "%d-%b-%Y"))
    print(fo_data['TIMESTAMP'].dtype)

    fo_data['spot'] = fo_data['SYMBOL'].map(cash_dict)

    fo_data.set_index("INSTRUMENT", inplace = True)

    if ('FUTIVX' in fo_data.index):
        fo_data.drop(['FUTIDX','FUTSTK','FUTIVX'],inplace=True)
    else:
        fo_data.drop(['FUTIDX', 'FUTSTK'], inplace=True)

    fo_data[['IV','Theta','Gamma','Delta','Vega','Rho']] = fo_data.apply(lambda row: pd.Series(get_greeks(row['spot'],row['STRIKE_PR'],row['EXPIRY_DT'],row['SETTLE_PR'],row['TIMESTAMP'],row['OPTION_TYP'])), axis=1)

    temp_file_name =  final_path + index_file[0:8] + '_greeks.csv'
    fo_data.to_csv(temp_file_name)
    print(index_file[0:8] + '_greeks.csv')
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="theraj4014",
                                   db="options"))

    fo_data.to_sql('greeks_data', con = engine, if_exists = 'append')

    end = time.time()
    print(end-start)
end = time.time()
print(end-start)


