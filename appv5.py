import threading
import logging
import requests
import numpy as np
import pandas as pd
from binance.client import Client
from binance.enums import *
from datetime import datetime, timedelta
import time
import pytz

# Configuration - Insert your Binance API keys here
BINANCE_API_KEY = 'P2pDI6cwqDW8wtr00wwbnQcVAJqv0WaCq0Nj1S1zVcKIShq9rcyDJHp8oneYaFSC'
BINANCE_API_SECRET = 'kcW4D5MdCyK084L5pl9Y8OyYCNBLJltlD6t05P3HLjDtfSjfPVU5sn8qFm5BzIWd'
LUNARCRUSH_API_KEY = '40a5jeizbairls0kvqfbzvsoddlzmpqu0m3hjuema'
MAX_API_CALLS = 9999999999999999  # 99% of the 1800 calls per day limit
TRADE_COOLDOWN_DAYS = 5
api_calls = 0

# Initialize Binance API
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Track last trade dates and placed orders
last_trade_dates = {}
placed_orders = {}  # Store orders with timestamps and order details

def get_kuwait_time():
    kuwait_tz = pytz.timezone('Asia/Kuwait')
    return datetime.now(kuwait_tz).strftime('%Y-%m-%d %H:%M:%S %Z')

# Function to map various symbol aliases to the standard format
def standardize_symbol(raw_symbol):
    symbol_aliases = {
    "BNB": ["BNB", "BNBUSDT", "Binance Coin"],
    "ADA": ["ADA", "ADAUSDT", "Cardano"],
    "SOL": ["SOL", "SOLUSDT", "Solana"],
    "XRP": ["XRP", "XRPUSDT", "Ripple"],
    "DOT": ["DOT", "DOTUSDT", "Polkadot"],
    "DOGE": ["DOGE", "DOGEUSDT", "Dogecoin"],
    "AVAX": ["AVAX", "AVAXUSDT", "Avalanche"],
    "SHIB": ["SHIB", "SHIBUSDT", "Shiba Inu"],
    "MATIC": ["MATIC", "MATICUSDT", "Polygon"],
    "LTC": ["LTC", "LTCUSDT", "Litecoin"],
    "UNI": ["UNI", "UNIUSDT", "Uniswap"],
    "BCH": ["BCH", "BCHUSDT", "Bitcoin Cash"],
    "LINK": ["LINK", "LINKUSDT", "Chainlink"],
    "TON": ["TON", "TONUSDT", "Toncoin"],
    "TRX": ["TRX", "TRXUSDT", "TRON"],
    "LEO": ["LEO", "LEOUSDT", "UNUS SED LEO"],
    "DAI": ["DAI", "DAIUSDT", "Dai"],
    "NEAR": ["NEAR", "NEARUSDT", "NEAR Protocol"],
}

    for key, aliases in symbol_aliases.items():
        if raw_symbol.upper() in [alias.upper() for alias in aliases]:
            return f"{key}USDT"  # Binance uses symbols without slashes, e.g., BNBUSDT
    return None

# Fetch data from LunarCrush with retries using API v4
def fetch_lunarcrush_endpoint(endpoint, topic, retries=3, delay=5):
    global api_calls
    if api_calls >= MAX_API_CALLS:
        print(f"[{get_kuwait_time()}] API call limit reached")
        return None

    url = f'https://lunarcrush.com/api4/public/{endpoint}/{topic}/v1'
    headers = {
        'Authorization': f'Bearer {LUNARCRUSH_API_KEY}'
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            print(f"[{get_kuwait_time()}] {response}")
            api_calls += 1
            if response.status_code == 200:
                time.sleep(delay)
                return response.json()
            else:
                logging.error(f"Error fetching LunarCrush data from {endpoint}: {response.status_code}")
                return None
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Connection error on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                return None
    return None

# Fetch summary, posts, and time series data for a topic
def fetch_lunarcrush_data(topic):
    summary_data = fetch_lunarcrush_endpoint('topic', topic)
    posts_data = fetch_lunarcrush_endpoint('topic', f'{topic}/posts')
    time_series_data = fetch_lunarcrush_endpoint('topic', f'{topic}/time-series')
    if summary_data and posts_data and time_series_data:
        return {
            'summary': summary_data,
            'posts': posts_data,
            'time_series': time_series_data
        }
    else:
        return None

# Get sentiment change within the last minute
def get_sentiment_change(topic):
    one_minute_ago = int((datetime.now() - timedelta(minutes=1)).timestamp())
    endpoint = f"public/topic/{topic}/v1?start={one_minute_ago}"
    sentiment_now = fetch_lunarcrush_endpoint(endpoint, topic)
    
    if sentiment_now:
        sentiment_score_now = sentiment_now['data']['types_sentiment'].get('tweet', 50)  # Default to neutral
        return sentiment_score_now
    return None

# Calculate ATR (Average True Range)
def calculate_atr(symbol):
    # Fetch crypto candles
    klines = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_30MINUTE, limit=14)
    
    # Convert the klines to a DataFrame
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                                       'close_time', 'quote_asset_volume', 'number_of_trades', 
                                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)

    df['high-low'] = df['high'] - df['low']
    df['high-close'] = np.abs(df['high'] - df['close'].shift())
    df['low-close'] = np.abs(df['low'] - df['close'].shift())
    df['tr'] = df[['high-low', 'high-close', 'low-close']].max(axis=1)
    atr = df['tr'].rolling(window=14).mean().iloc[-1]
    return atr

# Get current price from Binance
def get_current_price(symbol):
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker['price']) if ticker else None

# Determine trade side based on sentiment
def determine_trade_side(sentiment_score):
    side = None

    if sentiment_score >= 86:
        side = "BUY"
        print(f"[{get_kuwait_time()}] Trade side determined: buy (sentiment score >= 86)")
    elif sentiment_score < 55:
        side = "SELL"
        print(f"[{get_kuwait_time()}] Trade side determined: sell (sentiment score < 55)")

    if side is None:
        print(f"[{get_kuwait_time()}] No trade side determined based on sentiment score.")
        
    return side

# Helper function to get the lot size for a symbol
# def get_lot_size(symbol):
#     info = client.get_symbol_info(symbol)
#     for filt in info['filters']:
#         if filt['filterType'] == 'LOT_SIZE':
#             return float(filt['stepSize'])
#     return None

# Helper function to round quantity to appropriate precision
# def round_quantity(symbol, qty):
#     step_size = get_lot_size(symbol)
#     if step_size:
#         precision = int(round(-np.log10(step_size)))
#         return round(qty, precision)
#     return qty

# Helper function to format quantity to 3 decimal places
def format_quantity(qty):
    return float(f"{qty:.3f}")

# Helper function to format price to 3 decimal places
def format_price(price):
    return float(f"{price:.3f}")

# Place a market order on Binance
def place_order(symbol, qty, side):
    try:
        formatted_qty = format_quantity(qty)
        order = client.order_market(
            symbol=symbol,
            side=side,
            quantity=formatted_qty
        )
        logging.info(f"Market order placed: {side} {formatted_qty} of {symbol}")
        return order
    except Exception as e:
        logging.error(f"Error placing market order: {e}")
        return None
# Place an OCO Sell Order
def place_oco_order_sell(symbol, entry_price, atr, qty):
    stop_price = format_price(entry_price + atr)  # Stop price is higher than entry price
    stop_limit_price = format_price(stop_price * 1.005)  # Stop limit price is slightly higher
    take_profit_price = format_price(entry_price * 0.965)  # Take profit price is lower than entry price

    # Ensure the price relationships are correct
    if not (take_profit_price > entry_price > stop_price):
        logging.error(f"Price relationship error for OCO Sell Order: Limit Price > Last Price > Stop Price not maintained.")
        return None

    try:
        formatted_qty = format_quantity(qty)
        oco_order = client.order_oco_sell(
            symbol=symbol,
            quantity=formatted_qty,
            price=take_profit_price,
            stopPrice=stop_price,
            stopLimitPrice=stop_limit_price,
            stopLimitTimeInForce=TIME_IN_FORCE_GTC
        )
        logging.info(f"OCO sell order placed: take profit at {take_profit_price}, stop loss at {stop_price}")
        return oco_order
    except Exception as e:
        logging.error(f"Error placing OCO sell order: {e}")
        return None

# Place an OCO Buy Order
def place_oco_order_buy(symbol, entry_price, atr, qty):
    stop_price = format_price(entry_price - atr)  # Stop price is lower than entry price
    stop_limit_price = format_price(stop_price * 0.995)  # Stop limit price is slightly lower
    take_profit_price = format_price(entry_price * 1.035)  # Take profit price is higher than entry price

    # Ensure the price relationships are correct
    if not (take_profit_price < entry_price < stop_price):
        logging.error(f"Price relationship error for OCO Buy Order: Limit Price < Last Price < Stop Price not maintained.")
        return None

    try:
        formatted_qty = format_quantity(qty)
        oco_order = client.order_oco_buy(
            symbol=symbol,
            quantity=formatted_qty,
            price=take_profit_price,
            stopPrice=stop_price,
            stopLimitPrice=stop_limit_price,
            stopLimitTimeInForce=TIME_IN_FORCE_GTC
        )
        logging.info(f"OCO buy order placed: take profit at {take_profit_price}, stop loss at {stop_price}")
        return oco_order
    except Exception as e:
        logging.error(f"Error placing OCO buy order: {e}")
        return None


# Function to manage placed orders and execute post-order functions
def manage_placed_orders(symbol, side, entry_price, atr, qty):
    try:
        if side == "BUY":
            place_oco_order_sell(symbol, entry_price, atr, qty)
        elif side == "SELL":
            place_oco_order_buy(symbol, entry_price, atr, qty)
    except Exception as e:
        logging.error(f"Critical error managing placed orders for {symbol}: {e}")


# Function to handle 5-day cooldown and automatic sell after 5 days
def handle_cooldown_and_sell(cryptocurrencies):
    while True:
        current_time = datetime.now()
        for symbol in list(placed_orders.keys()):
            order_time = placed_orders[symbol]['time']
            side = placed_orders[symbol]['side']
            qty = placed_orders[symbol]['qty']

            if (current_time - order_time).days >= TRADE_COOLDOWN_DAYS:
                # If 5 days have passed, place a sell order
                if side == 'BUY':
                    try:
                        place_order(symbol, qty, 'SELL')
                        print(f"[{get_kuwait_time()}] Sell order placed for {symbol} after 5 days.")
                    except Exception as e:
                        logging.error(f"Error placing sell order for {symbol} after 5 days: {e}")

                # Re-add the symbol to the cryptocurrencies list
                cryptocurrencies.append(symbol)
                del placed_orders[symbol]

        time.sleep(60)  # Check every minute

# Main trading loop
def main_trading_loop(cryptocurrencies):
    while True:
        try:
            buy_signals = []
            sell_signals = []

            for crypto_symbol in cryptocurrencies:
                lunarcrush_data = fetch_lunarcrush_data(crypto_symbol)
                if lunarcrush_data:
                    data = lunarcrush_data['summary']['data']
                    types_sentiment = data.get('types_sentiment', {})
                    if types_sentiment:
                        sentiment_score = (types_sentiment['reddit-post'] + types_sentiment['tweet']) / 2
                        print(f"[{get_kuwait_time()}] Crypto: {crypto_symbol}, Sentiment Score: {sentiment_score}")

                        if sentiment_score >= 86:
                            buy_signals.append((crypto_symbol, sentiment_score))
                        elif sentiment_score < 55:
                            sell_signals.append((crypto_symbol, sentiment_score))

            if not buy_signals and not sell_signals:
                print(f"[{get_kuwait_time()}] No suitable cryptocurrencies found based on sentiment.")
            else:
                selected_cryptos = buy_signals + sell_signals
                print(f"[{get_kuwait_time()}] Selected Cryptocurrencies: {selected_cryptos}")

                for crypto_symbol, sentiment_score in selected_cryptos:
                    symbol = standardize_symbol(crypto_symbol)
                    if not symbol:
                        print(f"[{get_kuwait_time()}] Symbol not recognized.")
                        continue

                    if symbol in last_trade_dates and (datetime.now() - last_trade_dates[symbol]).days < TRADE_COOLDOWN_DAYS:
                        print(f"[{get_kuwait_time()}] Trade cooldown period not met for {symbol}.")
                        continue

                    # Try fetching data for each selected cryptocurrency
                    atr = calculate_atr(symbol)
                    if atr is None:
                        print(f"[{get_kuwait_time()}] Skipping {crypto_symbol} due to missing ATR data.")
                        continue  # Skip to the next cryptocurrency

                    current_price = get_current_price(symbol)
                    if current_price is None:
                        print(f"[{get_kuwait_time()}] Skipping {crypto_symbol} due to missing current price.")
                        continue  # Skip to the next cryptocurrency

                    entry_price = current_price
                    side = determine_trade_side(sentiment_score)
                    print(f"[{get_kuwait_time()}] Determined trade side for {crypto_symbol}: {side}")

                    if side:
                        # Check USDT balance and determine quantity to trade
                        balance = client.get_asset_balance(asset='USDT')
                        usdt_balance = float(balance['free'])

                        min_qty = 0.001  # Example minimum quantity, adjust as needed
                        qty = max(usdt_balance * 0.1 / entry_price, min_qty) if entry_price > 0 and usdt_balance > 0 else 0

                        if qty > min_qty:
                            try:
                                place_order(symbol, qty, side)
                                placed_orders[symbol] = {
                                    'time': datetime.now(),
                                    'side': side,
                                    'qty': qty,
                                    'entry_price': entry_price
                                }
                                cryptocurrencies.remove(crypto_symbol)
                                last_trade_dates[symbol] = datetime.now()

                                # Manage placed orders directly in the main thread (no multithreading)
                                manage_placed_orders(symbol, side, entry_price, atr, qty)
                            except Exception as e:
                                logging.error(f"Error during trade execution for {crypto_symbol}: {e}")
                                continue  # Move to the next cryptocurrency in case of error
                        else:
                            logging.error(f"Calculated qty is too low or invalid for {crypto_symbol}, order not placed.")
                            continue  # Move to the next cryptocurrency in case of invalid quantity

            time.sleep(60)

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            print(f"[{get_kuwait_time()}] Restarting the program in 30 seconds...")
            time.sleep(30)
            continue

if __name__ == "__main__":
    cryptocurrencies = [
         "BNB", "ADA", "SOL", "XRP", "DOT", 
        "DOGE", "AVAX", "SHIB", "MATIC", "LTC", "UNI", 
        "BCH", "LINK",  "TON", "TRX", "LEO", "DAI", "NEAR"
    ]

    # Start a thread to handle the 5-day cooldown and automatic sell condition
    threading.Thread(target=handle_cooldown_and_sell, args=(cryptocurrencies,)).start()

    # Start the main trading loop
    main_trading_loop(cryptocurrencies)
