from datetime import date, timedelta
from typing import List

import numpy
import pandas as pd

from plotly import tools
import plotly.offline as py
import plotly.graph_objs as go

from constants import Keys
from model import StrikeEntry
import database_connection as dbc, payoff_charts


def options_strategy(symbol: str, strike_data: List[StrikeEntry], expiry_month: int, expiry_year: int, start_date: date,
                     spot_range: list, strategy_name: str = None):
    """
    Used for the analysis and back testing of the strikes for the period of the expiry.
    It plots theoretical payoffs along with the individual payoff for each strike over the time of expiry.
    If a start date is not given then first day of the expiry month is taken.
    :param symbol: str
                Symbol for which analysis is to be done.
    :param strike_data: list[StrikeEntry]
                List of strikes for the analysis.
    :param expiry_month: int
                Expiry month for which back testing is to be done.
                For e.g. For month of 'October', 10 is input.
    :param expiry_year: int
                Year of the expiry month. This is included in case if database expands over multiple years.
                For e.g. 2018
    :param start_date: date
                Start date for the back testing. If none given, first of month is taken.
    :param spot_range: list
                Values required for the calculating theoretical payoffs. for eg. [9500, 11000]
    :param strategy_name: str
                Name of the strategy
    :return: None
                Plots the different payoffs for the strategy inputs.
    """
    symbol = symbol.upper()

    option_query = "Select * from %s where symbol='%s' and instrument like 'OPT%%' and MONTH(EXPIRY_DT)=%d and YEAR(EXPIRY_DT)=%d" % (
        dbc.table_name, symbol, expiry_month, expiry_year)
    option_data = dbc.execute_simple_query(option_query)
    option_df = pd.DataFrame(data=option_data, columns=dbc.columns)
    payoff_data = []
    for strikes in strike_data:
        if type(strikes) == StrikeEntry:
            strike = [strikes.strike]
            option_type = [strikes.option_type]
            strike_df = option_df[option_df.strike.isin(strike) & option_df.option_typ.isin(option_type)]
            init_day_entry = strike_df[strike_df.timestamp == start_date]
            if (len(init_day_entry) > 0) & (strikes.signal in [Keys.buy, Keys.sell]):
                init_price = init_day_entry.close.values[0]
                for row in strike_df.itertuples():
                    timestamp = row.timestamp
                    close = row.close
                    if timestamp >= start_date:
                        temp_pl = close - init_price
                        pl = temp_pl if strikes.signal == Keys.buy else (-1 * temp_pl)
                        payoff_data.append([timestamp, strikes.strike, strikes.option_type, pl])
            else:
                print("Couldn't find initial price for strike: %s%s and start date: %s" % (
                    strikes.strike, strikes.option_type, start_date))
        else:
            print("Input can be only be of type %s given %s" % (StrikeEntry, type(strikes)))
            return

    if len(payoff_data) > 0:
        payoff_df = pd.DataFrame(payoff_data, columns=['timestamp', 'strike', 'option_typ', 'pl'])
        timestamp_cum_pl = [[], []]
        payoff_timestamp = payoff_df.timestamp.unique()
        for data_timestamp in payoff_timestamp:
            timestamp = [data_timestamp]
            timestamp_df = payoff_df[payoff_df.timestamp.isin(timestamp)]
            timestamp_pl = timestamp_df.pl.sum()
            timestamp_cum_pl[0].append(data_timestamp)
            timestamp_cum_pl[1].append(timestamp_pl)

        strike_cum_pl = []
        for strikes in strike_data:
            strike_time_series = [[], []]
            strike = [strikes.strike]
            option_type = [strikes.option_type]
            strike_payoff_df = payoff_df[payoff_df.strike.isin(strike) & payoff_df.option_typ.isin(option_type)]
            for item in strike_payoff_df.itertuples():
                strike_time_series[0].append(item.timestamp)
                strike_time_series[1].append(item.pl)
            strike_info = dict(
                strike=strikes.strike,
                option_type=strikes.option_type,
                signal=strikes.signal,
                timeseries=strike_time_series,
                df=strike_payoff_df,
            )
            strike_cum_pl.append(strike_info)

        spot, theoretical_payoff = _get_theoretical_payoffs(spot_range, strike_data)
        _plot_options_strategy_payoffs(symbol, timestamp_cum_pl, strike_cum_pl,
                                       [spot, theoretical_payoff], strategy_name)


def _get_theoretical_payoffs(spot: list, strike_data: list):
    """
    Helper function for getting theoretical payoffs
    :param spot: list
            Range of Spot values
    :param strike_data: list[StrikeEntry]
            Strike entry for the theoretical payoffs
    :return: tuple(list, list)
            Returns spot values and corresponding payoffs
    """
    spot = numpy.arange(min(spot), max(spot), 100, dtype=numpy.int64).tolist()
    payoff = []
    for strike in strike_data:
        premium = strike.premium if strike.premium else 100
        if premium:
            payoff_list = payoff_charts._get_payoff_values(spot, strike.strike, strike.option_type, premium,
                                                           signal=strike.signal)
            print(payoff_list)
            payoff.append(payoff_list)
        else:
            print("Parameter premium is required for %s for theoretical payoffs" % strike)

    while len(payoff) > 1:
        one = payoff[0]
        two = payoff[1]
        for i in range(len(one)):
            one[i] = one[i] + two[i]
        payoff.pop(1)

    if len(payoff) == 1:
        payoff = payoff[0]
    return spot, payoff


def _plot_options_strategy_payoffs(symbol, timestamp_cum_pl, strike_cum_pl, theoretical_pl,
                                   strategy_name: str = None):
    """
    Helper function for plotting payoff diagrams in option strategy
    :param symbol: str
                Symbol under analysis
    :param fut_timeseries_data: list
                Data for plotting underlying future price
    :param timestamp_cum_pl: list
                Data for plotting cumulative profit and loss
    :param strike_cum_pl: list
                Data for plotting individual strike profit and loss
    :param theoretical_pl: list
                Data for plotting theoretical payoffs
    :param strategy_name: str
                Name of the strategy
    :return: None
                Plots the input data
    """
    titles = []
    traces = []
    # fut_period = fut_timeseries_data[0]
    # fut_values = fut_timeseries_data[1]

    period = timestamp_cum_pl[0]
    values = timestamp_cum_pl[1]

    spot = theoretical_pl[0]
    payoff = theoretical_pl[1]

    # if fut_period:
    #     name = 'Underlying %s' % symbol
    #     trace_fut = go.Scatter(x=fut_period, y=fut_values, name=symbol)
    #     titles.append(name)
    #     traces.append(trace_fut)

    if spot:
        name = 'Theoretical Payoffs'
        trace_payoff = go.Scatter(x=spot, y=payoff, name=name)
        titles.append(name)
        traces.append(trace_payoff)

    if period:
        name = 'Cumulative P&L'
        trace_pl = go.Scatter(x=period, y=values, name=name)
        titles.append(name)
        traces.append(trace_pl)

    for strike_pl in strike_cum_pl:
        strike = strike_pl['strike']
        opt_type = strike_pl['option_type']
        signal = strike_pl['signal']
        df = strike_pl['df']
        name = '%s%s' % (strike, opt_type)
        trace = go.Scatter(x=df['timestamp'], y=df['pl'], name=name)
        titles.append('%s %s' % (name, signal))
        traces.append(trace)

    columns = 3
    len_traces = len(traces)
    rows = int(len_traces / columns) if len_traces % columns == 0 else (int(len_traces / columns) + 1)
    fig = tools.make_subplots(rows=rows, cols=columns, subplot_titles=titles)

    i = 0
    for row in range(rows):
        for col in range(columns):
            if i < len_traces:
                fig.append_trace(traces[i], row=row + 1, col=col + 1)
                i += 1

    title_name = '%s_payoffs' % (strategy_name if strategy_name else 'option_strategy')
    fig['layout'].update(title=title_name.upper())

    py.plot(fig, filename='%s.html' % title_name)
