from datetime import timedelta
from enum import Enum

# REST API Endpoints
# Spot End Points
AMBERDATA_SPOT_REST_OHLCV_ENDPOINT = "https://api.amberdata.com/markets/spot/ohlcv/"
AMBERDATA_SPOT_REST_LEGACY_OHLCV_ENDPOINT = "https://api.amberdata.com/market/spot/ohlcv/"
AMBERDATA_SPOT_REST_TRADES_ENDPOINT = "https://api.amberdata.com/markets/spot/trades/"
AMBERDATA_SPOT_REST_PRICES_ENDPOINT = "https://api.amberdata.com/market/spot/prices/"
AMBERDATA_SPOT_REST_EXCHANGES_ENDPOINT = "https://api.amberdata.com/markets/spot/exchanges/information/"
AMBERDATA_SPOT_REST_PAIRS_ENDPOINT = "https://api.amberdata.com/market/spot/prices/pairs/information/"
AMBERDATA_SPOT_REST_EXCHANGES_REFERENCE_ENDPOINT = "https://api.amberdata.com/markets/spot/exchanges/reference/"
AMBERDATA_SPOT_REST_REFERENCE_RATES_ENDPOINT = "https://api.amberdata.com/markets/spot/reference-rates/"
AMBERDATA_SPOT_REST_TICKERS_ENDPOINT = "https://api.amberdata.com/markets/spot/tickers/"
AMBERDATA_SPOT_REST_TWAP_ENDPOINT = "https://api.amberdata.com/market/spot/twap/"
AMBERDATA_SPOT_REST_ORDER_BOOK_EVENTS_ENDPOINT = "https://api.amberdata.com/markets/spot/order-book-events/"
AMBERDATA_SPOT_REST_ORDER_BOOK_SNAPSHOTS_ENDPOINT = "https://api.amberdata.com/markets/spot/order-book-snapshots/"
AMBERDATA_SPOT_REST_VWAP_ENDPOINT = "https://api.amberdata.com/market/spot/vwap/"

# Futures End Points
AMBERDATA_FUTURES_REST_EXCHANGES_ENDPOINT = "https://api.amberdata.com//markets/futures/exchanges/"
AMBERDATA_FUTURES_REST_FUNDING_RATES_ENDPOINT = "https://api.amberdata.com/markets/futures/funding-rates/"
AMBERDATA_FUTURES_REST_BATCH_FUNDING_RATES_ENDPOINT = "https://api.amberdata.com/market/futures/funding-rates/"
AMBERDATA_FUTURES_REST_INSURANCE_FUNDS_ENDPOINT = "https://api.amberdata.com/markets/futures/insurance-fund/"
AMBERDATA_FUTURES_REST_LIQUIDATIONS_ENDPOINT = "https://api.amberdata.com/markets/futures/liquidations/"
AMBERDATA_FUTURES_REST_LONG_SHORT_RATIO_ENDPOINT = "https://api.amberdata.com/markets/futures/long-short-ratio/"
AMBERDATA_FUTURES_REST_OHLCV_ENDPOINT = "https://api.amberdata.com/markets/futures/ohlcv/"
AMBERDATA_FUTURES_REST_BATCH_OHLCV_ENDPOINT = "https://api.amberdata.com/market/futures/ohlcv/"
AMBERDATA_FUTURES_REST_OPEN_INTEREST_ENDPOINT = "https://api.amberdata.com/markets/futures/open-interest/"
AMBERDATA_FUTURES_REST_BATCH_OPEN_INTEREST_ENDPOINT = "https://api.amberdata.com/market/futures/open-interest/"
AMBERDATA_FUTURES_REST_ORDER_BOOK_SNAPSHOTS_ENDPOINT = "https://api.amberdata.com/markets/futures/order-book-snapshots/"
AMBERDATA_FUTURES_REST_ORDER_BOOK_EVENTS_ENDPOINT = "https://api.amberdata.com/markets/futures/order-book-events/"
AMBERDATA_FUTURES_REST_TICKERS_ENDPOINT = "https://api.amberdata.com/markets/futures/tickers/"
AMBERDATA_FUTURES_REST_TRADES_ENDPOINT = "https://api.amberdata.com/markets/futures/trades/"

# Swaps End Points
AMBERDATA_SWAPS_REST_BATCH_FUNDING_RATES_ENDPOINT = "https://api.amberdata.com/market/swaps/funding-rates/"
AMBERDATA_SWAPS_REST_BATCH_OHLCV_ENDPOINT = "https://api.amberdata.com/market/swaps/ohlcv/"
AMBERDATA_SWAPS_REST_BATCH_OPEN_INTEREST_ENDPOINT = "https://api.amberdata.com/market/swaps/open-interest/"

# Defi End Points
AMBERDATA_DEFI_REST_INFORMATION_ENDPOINT = "https://web3api.io/api/v2/market/defi/"
AMBERDATA_DEFI_REST_LENDING_ENDPOINT = "https://web3api.io/api/v2/market/defi/lending/exchanges/"
AMBERDATA_DEFI_REST_LIQUIDITY_ENDPOINT = "https://web3api.io/api/v2/market/defi/liquidity/"
AMBERDATA_DEFI_REST_LIQUIDITY_POOLS_ENDPOINT = "https://web3api.io/api/v2/market/defi/liquidity-positions/"
AMBERDATA_DEFI_REST_OHLCV_ENDPOINT = "https://web3api.io/api/v2/market/defi/ohlcv"
AMBERDATA_DEFI_REST_METRICS_ENDPOINT = "https://web3api.io/api/v2/market/defi/metrics/"
AMBERDATA_DEFI_REST_TRADES_ENDPOINT = "https://web3api.io/api/v2/market/defi/trades/"
AMBERDATA_DEFI_REST_TWAP_ENDPOINT = "https://web3api.io/api/v2/market/defi/twap/"
AMBERDATA_DEFI_REST_VWAP_ENDPOINT = "https://web3api.io/api/v2/market/defi/vwap/"
AMBERDATA_DEFI_REST_PRICES_ENDPOINT = "https://web3api.io/api/v2/market/defi/prices/"


class MarketDataVenue(str, Enum):
    BINANCE = "binance"
    BINANCEUS = "binanceus"
    BITFINEX = "bitfinex"
    BITGET = "bitget"
    BITHUMB = "bithumb"
    BITMEX = "bitmex"
    BITSTAMP = "bitstamp"
    BYBIT = "bybit"
    CBOEDIGITAL = "cboedigital"
    COINBASE = "gdax"
    GDAX = "gdax"
    GEMINI = "gemini"
    HUOBI = "huobi"
    ITBIT = "itbit"
    KRAKEN = "kraken"
    LMAX = "lmax"
    MERCADOBITCOIN = "mercadobitcoin"
    MEXC = "mexc"
    OKEX = "okex"
    POLONIEX = "poloniex"
    DERIBIT = "deribit"

class TimeFormat(Enum):
    MILLISECONDS = "milliseconds"
    MS = "ms"
    ISO = "iso"
    ISO8601 = "iso8601"
    HR = "hr"
    HUMAN_READABLE = "human_readable"

class DexDataVenue(str, Enum):
    UNISWAP_V2 = "uniswapv2"
    UNISWAP_V3 = "uniswapv3"
    SUSHISWAP = "sushiswap"
    BALANCER_VAULT = "balancer vault"
    CURVE_V1 = "curvev1"
    PANCAKESWAP = "Pancake LPs"
    CRODEFISWAP = "CroDefiSwap"

class LendingProtocol(str, Enum):
    AAVE_V1 = "aave_v1"
    AAVE_V2 = "aave_v2"
    AAVE_V3 = "aave_v3"
    COMPOUND_V1 = "compound_v1"
    CREAM_V1 = "cream_v1"
    CREAM_V2 = "cream_v2"
    FORTUBE_V1 = "fortube_v1"


class TimeInterval(Enum):
    MINUTE = 'minutes'
    HOUR = 'hours'
    DAY = 'days'
    TICKS = 'ticks'


class BatchPeriod(Enum):
    HOUR_1 = timedelta(hours=1)
    HOUR_2 = timedelta(hours=2)
    HOUR_4 = timedelta(hours=4)
    HOUR_8 = timedelta(hours=8)
    HOUR_12 = timedelta(hours=12)
    HOUR_16 = timedelta(hours=16)
    HOUR_20 = timedelta(hours=20)
    DAY_1 = timedelta(days=1)
    DAY_3 = timedelta(days=3)
    DAY_7 = timedelta(days=7)

class TimeBucket(Enum):
    MINUTES_5 = '5m'
    HOURS_1 = '1h'
    DAYS_1 = '1d'

class SortBy(Enum):
    NAME = 'name'
    NUMPAIRS = 'numPairs'

class SortDirection(Enum):
    ASCENDING = 'asc'
    DESCENDING = 'desc'

class DailyTime(Enum):
    T1600_M0500 = "T16:00-05:00"
    T1600_M0400 = "T16:00-04:00"
    T1600_P0000 = "T16:00+00:00"
    T1600_P0100 = "T16:00+01:00"
    T1600_P0400 = "T16:00+04:00"
    T1600_P0800 = "T16:00+08:00"
    T1600_P0900 = "T16:00+09:00"
