import threading
import logging
import requests
import numpy as np
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from datetime import datetime, timedelta
import time
import pytz

# Configuration - Insert your API keys here
ALPACA_API_KEY = 'AKYI9P9DGWN060ID3S8S'
ALPACA_API_SECRET = 'Nj9YhfyfGUN0SNE0zxEggeCxZadY6RFU5Nf1rH6s'
ALPACA_BASE_URL = "https://api.alpaca.markets"
LUNARCRUSH_API_KEY = '40a5jeizbairls0kvqfbzvsoddlzmpqu0m3hjuema'
MAX_API_CALLS = 9999999999999999  # 99% of the 1800 calls per day limit
TRADE_COOLDOWN_DAYS = 5
api_calls = 0

# Initialize Alpaca API
alpaca = REST(ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_BASE_URL, api_version='v2')

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
    "USDC": ["USDC", "USDCUSDT", "USD Coin"],
    "TON": ["TON", "TONUSDT", "Toncoin"],
    "TRX": ["TRX", "TRXUSDT", "TRON"],
    "LEO": ["LEO", "LEOUSDT", "UNUS SED LEO"],
    "DAI": ["DAI", "DAIUSDT", "Dai"],
    "NEAR": ["NEAR", "NEARUSDT", "NEAR Protocol"],
}

    for key, aliases in symbol_aliases.items():
        if raw_symbol.upper() in [alias.upper() for alias in aliases]:
            return f"{key}/USDT"
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
    # Fetch crypto bars
    bars = alpaca.get_crypto_bars(symbol, TimeFrame(30, TimeFrameUnit.Minute), limit=14).df
    
    # Check the structure and content of the DataFrame
    print(f"[{get_kuwait_time()}] DataFrame Structure:")
    print(f"[{get_kuwait_time()}] {bars.head()}")    
    # Ensure that the required columns exist
    if 'high' not in bars.columns or 'low' not in bars.columns or 'close' not in bars.columns:
        logging.error(f"Required columns are missing in the data for {symbol}. Columns found: {bars.columns}")
        return None

    # Calculate the ATR if the columns exist
    df = pd.DataFrame(bars)
    df['high-low'] = df['high'] - df['low']
    df['high-close'] = np.abs(df['high'] - df['close'].shift())
    df['low-close'] = np.abs(df['low'] - df['close'].shift())
    df['tr'] = df[['high-low', 'high-close', 'low-close']].max(axis=1)
    atr = df['tr'].rolling(window=14).mean().iloc[-1]
    return atr

# Get current price from Alpaca
def get_current_price(symbol):
    # Fetch the quotes for the given symbol
    quotes = alpaca.get_crypto_quotes(symbol)
    
    # Check if quotes list is not empty
    if len(quotes) > 0:
        # Extract the first quote and return its bid price
        return quotes[0].bp
    else:
        logging.error(f"No quotes found for {symbol}")
        return None
    
# Determine trade side based on sentiment
def determine_trade_side(sentiment_score):
    side = None

    if sentiment_score >= 86:
        side = "buy"
        print(f"[{get_kuwait_time()}] Trade side determined: buy (sentiment score >= 86)")
    elif sentiment_score < 55:
        side = "sell"
        print(f"[{get_kuwait_time()}] Trade side determined: sell (sentiment score < 55)")

    if side is None:
        print(f"[{get_kuwait_time()}] No trade side determined based on sentiment score.")
        
    return side


# Get overall sentiment
def get_overall_sentiment(topic):
    global api_calls  # Declare that you want to use the global api_calls variable
    url = f'https://lunarcrush.com/api4/public/topic/{topic}/v1'
    headers = {
        'Authorization': f'Bearer {LUNARCRUSH_API_KEY}'
    }

    response = requests.get(url, headers=headers)
    api_calls += 1  # Increment the global api_calls variable
    if response.status_code == 200:
        data = response.json()['data']
        
        # Extract sentiment values for tweet and reddit-post
        tweet_sentiment = data['types_sentiment'].get('tweet', 0)
        reddit_sentiment = data['types_sentiment'].get('reddit-post', 0)
        
        # Calculate the average sentiment between tweet and reddit-post
        average_sentiment = (tweet_sentiment + reddit_sentiment) / 2
        return average_sentiment
    return 0

# Place an order on Alpaca
def place_order(symbol, qty, side):
    try:
        alpaca.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='gtc'
        )
        logging.info(f"Order placed: {side} {qty} of {symbol}")
    except Exception as e:
        logging.error(f"Error placing order: {e}")

# Adjust stop loss based on ATR
def adjust_stop_loss(symbol, side, entry_price, atr, qty):
    stop_price = entry_price - atr if side is not None and side.lower() == "buy" else entry_price + atr
    try:
        alpaca.submit_order(
            symbol=symbol,
            qty=qty,
            side='sell' if side.lower() == 'buy' else 'buy',
            type='stop',
            stop_price=stop_price,
            time_in_force='gtc'
        )
        logging.info(f"Stop loss set at {stop_price}")
    except Exception as e:
        logging.error(f"Error setting stop loss: {e}")


# Place take profit orders
def place_take_profit_orders(symbol, qty, side, entry_price):
    try:
        # First take profit order at 4% gain
        take_profit_1 = entry_price * 1.035 if side.lower() == "buy" else entry_price * 0.965
        alpaca.submit_order(
            symbol=symbol,
            qty=qty * 0.5,
            side='sell' if side.lower() == 'buy' else 'buy',
            type='limit',
            limit_price=take_profit_1,
            time_in_force='gtc'
        )
        logging.info(f"First take profit order placed at {take_profit_1}")

        # # Adjust stop loss to entry price after 4% gain
        # stop_loss_adjusted = entry_price
        # alpaca.submit_order(
        #     symbol=symbol,
        #     qty=qty * 0.5,
        #     side='sell' if side.lower() == 'buy' else 'buy',
        #     type='stop',
        #     stop_price=stop_loss_adjusted,
        #     time_in_force='gtc'
        # )
        # logging.info(f"Stop loss adjusted to entry price {entry_price}")

        # # Second take profit order at 7% gain
        # take_profit_2 = entry_price * 1.07 if side.lower() == "buy" else entry_price * 0.93
        # alpaca.submit_order(
        #     symbol=symbol,
        #     qty=qty * 0.3,
        #     side='sell' if side.lower() == 'buy' else 'buy',
        #     type='limit',
        #     limit_price=take_profit_2,
        #     time_in_force='gtc'
        # )
        # logging.info(f"Second take profit order placed at {take_profit_2}")

        # # Final take profit order at 10% gain
        # take_profit_3 = entry_price * 1.10 if side.lower() == "buy" else entry_price * 0.90
        # alpaca.submit_order(
        #     symbol=symbol,
        #     qty=qty * 0.2,
        #     side='sell' if side.lower() == 'buy' else 'buy',
        #     type='limit',
        #     limit_price=take_profit_3,
        #     time_in_force='gtc'
        # )
        # logging.info(f"Final take profit order placed at {take_profit_3}")

    except Exception as e:
        logging.error(f"Error placing take profit orders: {e}")

# Function to manage placed orders and execute post-order functions
def manage_placed_orders(symbol, side, entry_price, atr, qty):
    try:
        adjust_stop_loss(symbol, side, entry_price, atr, qty)
        place_take_profit_orders(symbol, qty, side, entry_price)
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
                if side == 'buy':
                    try:
                        place_order(symbol, qty, 'sell')
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
                        positions = alpaca.list_positions()
                        usdtusd_position = next((pos for pos in positions if pos.symbol == 'USDTUSD'), None)
                        usdt_balance = float(usdtusd_position.market_value) if usdtusd_position else 0.0

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
                                break  # Exit the loop after placing an order successfully
                            except Exception as e:
                                logging.error(f"Error during trade execution for {crypto_symbol}: {e}")
                        else:
                            logging.error(f"Calculated qty is too low or invalid for {crypto_symbol}, order not placed.")

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
        "BCH", "LINK", "USDC", "TON", "TRX", "LEO", "DAI", "NEAR"
    ]

    # Start a thread to handle the 5-day cooldown and automatic sell condition
    threading.Thread(target=handle_cooldown_and_sell, args=(cryptocurrencies,)).start()

    # Start the main trading loop
    main_trading_loop(cryptocurrencies)
