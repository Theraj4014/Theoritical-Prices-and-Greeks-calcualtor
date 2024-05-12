"""
Constants used in the modules
Don't change these values. It might break the code.
"""
import logging
from enum import Enum

default = "null"
log_level = logging.DEBUG


class StrikeEntry:
    """
    This is used to make a option's entry for analysis
    """

    def __init__(self, strike: int, option_type: str, signal: str = None, premium: float = None):
        """
        It creates an instance which refers to different properties of the option.
        :param strike: int
                    Strike price of the option
        :param option_type: str
                    Type of the option. Possible values CE and PE.
        :param signal: str
                    Signal for the option. Either 'buy' or 'sell'
        :param premium: float
                    Premium paid or received for the option.
        """
        self.strike = strike
        self.option_type = option_type
        self.signal = signal
        self.premium = premium

    def __str__(self) -> str:
        return "%s %s%s" % (self.signal, self.strike, self.option_type)


# TODO: 1.1 Constants used for call and put
class Keys:
    call = "CE"
    put = "PE"
    buy = "buy"
    sell = "sell"


class DbIndex(Enum):
    """
        This enums are used for the indexing the database columns for FO data
    """
    index_id = 0
    instrument_id = 1
    symbol_id = 2
    expiry_id = 3
    strike_id = 4
    option_type_id = 5
    open_id = 6
    high_id = 7
    low_id = 8
    close_id = 9
    settle_id = 10
    contracts_id = 11
    val_id = 12
    open_int_id = 13
    chg_in_oi = 14
    timestamp_id = 15

    def __str__(self):
        return self.value
