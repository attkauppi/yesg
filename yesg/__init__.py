from .main import YahooESGClient

_client = YahooESGClient()

def get_esg_short(ticker):
    return _client.get_esg_short(ticker)

def get_esg_full(ticker):
    return _client.get_esg_full(ticker)

def get_historic_esg(ticker):
    return _client.get_historic_esg(ticker)

__all__ = ["get_esg_short", "get_esg_full", "get_historic_esg", "YahooESGClient"]