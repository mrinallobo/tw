import asyncio
from ib_insync import *
import numpy as np
from datetime import datetime, timedelta, time
import logging
import pandas as pd
import pandas_ta as ta
import pytz

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    filename='test.log',  # File where logs will be written
    filemode='w'  # Open in append mode
)

# Constants
ORB_LENGTH = 30  # ORB length in minutes (variable)
CONSOLIDATION_TIME = 15  # Consolidation time in minutes
MAX_CLOSE_BELOW_VWAP = 5  # Maximum number of candle closes below VWAP
STOP_LOSS_PERCENTAGE = 0.003  # Stop loss percentage (0.3%)
CLOSE_TIME = datetime.strptime('15:45', '%H:%M').time()  # Close time (15 minutes before market close)
LATEST_BUY_TIME = time(16, 0)  # Latest time for initiating a buy order (2:00 PM)

ib = IB()
common_timezone = pytz.timezone('US/Eastern')

def make_offset_aware(dt, timezone):
    if dt.tzinfo is None:
        return timezone.localize(dt)
    return dt.astimezone(timezone)

def make_offset_aware(dt, timezone):
    if dt.tzinfo is None:
        return timezone.localize(dt)
    return dt.astimezone(timezone)

async def calculate_sma_slope(contract, sample_size, days_ago, bar_size):
    try:
        if sample_size == 'H':
            current_bars = await ib.reqHistoricalDataAsync(
                contract, '', '10 D', '1 hour', 'TRADES', useRTH=True)
            if not current_bars or len(current_bars) < 24:  # 10 days * 24 hours
                logging.warning("Insufficient historical data returned for current period.")
                return None
            current_10h_close = np.mean([bar.close for bar in current_bars[-10:]])
            past_10h_close = np.mean([bar.close for bar in current_bars[-20:-10]])
            slope = (current_10h_close - past_10h_close) / 10
            logging.info(f"Calculated SMA slope for 10 hours using hourly bars: {slope}")
            return slope
        else:
            current_end_time = datetime.now().strftime('%Y%m%d %H:%M:%S')
            current_bars = await ib.reqHistoricalDataAsync(
                contract, current_end_time, f"{days_ago} {sample_size}", bar_size, 'TRADES', useRTH=True)
            if not current_bars:
                logging.warning(f"No historical data returned for current period of {days_ago} {sample_size}.")
                return None
            current_sma = np.mean([bar.close for bar in current_bars])
            past_start_time = (datetime.now() - timedelta(days=days_ago*2)).strftime('%Y%m%d %H:%M:%S')
            past_bars = await ib.reqHistoricalDataAsync(
                contract, past_start_time, f"{days_ago} {sample_size}", bar_size, 'TRADES', useRTH=True)
            if not past_bars:
                logging.warning(f"No historical data returned for past period of {days_ago} {sample_size}.")
                return None
            past_sma = np.mean([bar.close for bar in past_bars])
            slope = (current_sma - past_sma) / days_ago
            logging.info(f"Calculated SMA slope for {days_ago} {sample_size} using {bar_size} bars: {slope}")
            return slope
    except Exception as e:
        logging.error(f"Error in calculating SMA slope: {e}")
        return None

async def check_sma_slopes(contract):
    try:
        periods = {
            '200 D': ('D', 200, '1 day'),
            '100 D': ('D', 100, '1 day'),
            '20 D': ('D', 20, '1 day'),
            '10 H': ('H', 10, '1 hour')
        }
        slopes = {}
        for period, (sample_size, days_ago, bar_size) in periods.items():
            slope = await calculate_sma_slope(contract, sample_size, days_ago, bar_size)
            if slope is None:
                logging.warning(f"SMA slope for {period} is not available.")
                return False
            slopes[period] = slope
        result = any(s > 0 for s in slopes.values())
        logging.info(f"All SMA slopes are positive: {result}")
        return result
    except Exception as e:
        logging.error(f"Error in check_sma_slopes: {e}")
        return False




async def find_max_price(contract, duration_minutes, lookback_minutes=5, forward_minutes=15):
    duration = f"{duration_minutes*60} S"
    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime='',
        durationStr=duration,
        barSizeSetting='1 min',
        whatToShow='TRADES',
        useRTH=True
    )
    logging.info(bars)
    if bars:
        df = pd.DataFrame([(bar.date, bar.high, bar.low, bar.close, bar.volume) for bar in bars],
                          columns=['date', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df['vwap'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])

        max_price = None
        max_time = None
        reset_threshold = False
        consolidation_start = None
        breakout_level = None

        for i in range(lookback_minutes, len(df) - forward_minutes):
            high_5min = df['high'].iloc[i-lookback_minutes:i].max()
            high_15min = df['high'].iloc[i+1:i+forward_minutes+1].max()
            current_high = df['high'].iloc[i]
            
            logging.info(f"Iteration {i}: Current high {current_high}, 5min max {high_5min}, 15min max {high_15min}")
            
            if current_high > high_5min and current_high > high_15min:
                close_below_vwap_count = (df['close'].iloc[i+1:i+6] < df['vwap'].iloc[i+1:i+6]).sum()
                
                logging.info(f"Potential max found at index {i}: Current high {current_high}, VWAP reset count: {close_below_vwap_count}")
                
                if close_below_vwap_count > 5:
                    logging.info(f"Reset triggered at index {i}, high: {current_high}, VWAP reset count: {close_below_vwap_count}")
                    reset_threshold = True
                    max_price = None
                    max_time = None
                    consolidation_start = None
                    breakout_level = None
                else:
                    if max_price is None or current_high > max_price:
                        max_price = current_high
                        max_time = df.index[i]
                        logging.info(f"New Max Found: {max_price} at {max_time}")
                        consolidation_start = df.index[i]
                        breakout_level = max_price
            else:
                logging.debug(f"No max condition met at index {i}: Current high {current_high}, 5min max {high_5min}, 15min max {high_15min}")
                
                if consolidation_start is not None and (df.index[i] - consolidation_start).total_seconds() / 60 >= 15:
                    close_below_vwap_count = (df['close'].iloc[i-14:i+1] < df['vwap'].iloc[i-14:i+1]).sum()
                    
                    logging.info(f"Consolidation period reached at index {i}, duration: {(df.index[i] - consolidation_start).total_seconds() / 60} minutes, VWAP count: {close_below_vwap_count}")
                    
                    if close_below_vwap_count <= 5 and current_high > breakout_level:
                        logging.info(f"Breakout confirmed at index {i}, breakout level: {breakout_level}")
                        return max_price, max_time
                    else:
                        logging.info(f"Consolidation violated at index {i}, VWAP count: {close_below_vwap_count}, breakout level: {breakout_level}")
                        consolidation_start = None
                        breakout_level = None

        if max_price and not reset_threshold:
            return max_price, max_time

    logging.warning("No data found to determine max price or reset condition met.")
    return None, None



# async def find_max_price(contract, duration_minutes, lookback_minutes=5, forward_minutes=15):
#     duration = f"{duration_minutes*16*60} S"
#     bars = await ib.reqHistoricalDataAsync(
#         contract,
#         endDateTime='',
#         durationStr=duration,
#         barSizeSetting='1 min',
#         whatToShow='TRADES',
#         useRTH=True
#     )
#     logging.info(bars)
#     if bars:
#         df = pd.DataFrame([(bar.date, bar.high, bar.low, bar.close, bar.volume) for bar in bars],
#                           columns=['date', 'high', 'low', 'close', 'volume'])
#         df['date'] = pd.to_datetime(df['date'])
#         df.set_index('date', inplace=True)
#         df['vwap'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])

#         max_price = None
#         max_time = None
#         reset_threshold = False
#         consolidation_start = None
#         breakout_level = None

#         # Find the overall maximum price in the data set
#         overall_max_price = df['high'].max()
#         overall_max_time = df[df['high'] == overall_max_price].index[0]
#         logging.info(f"Overall Max Price: {overall_max_price} at {overall_max_time}")

#         for i in range(lookback_minutes, len(df) - forward_minutes):
#             high_5min = df['high'].iloc[i-lookback_minutes:i].max()
#             high_15min = df['high'].iloc[i+1:i+forward_minutes+1].max()
#             current_high = df['high'].iloc[i]
            
#             logging.info(f"Iteration {i}: Current high {current_high}, 5min max {high_5min}, 15min max {high_15min}")
            
#             if current_high > high_5min and current_high > high_15min:
#                 close_below_vwap_count = (df['close'].iloc[i+1:i+6] < df['vwap'].iloc[i+1:i+6]).sum()
                
#                 logging.info(f"Potential max found at index {i}: Current high {current_high}, VWAP reset count: {close_below_vwap_count}")
                
#                 if close_below_vwap_count > 5:
#                     logging.info(f"Reset triggered at index {i}, high: {current_high}, VWAP reset count: {close_below_vwap_count}")
#                     reset_threshold = True
#                     max_price = None
#                     max_time = None
#                     consolidation_start = None
#                     breakout_level = None
#                 else:
#                     if max_price is None or current_high > max_price:
#                         max_price = current_high
#                         max_time = df.index[i]
#                         logging.info(f"New Max Found: {max_price} at {max_time}")
#                         consolidation_start = df.index[i]
#                         breakout_level = max_price
#             else:
#                 logging.debug(f"No max condition met at index {i}: Current high {current_high}, 5min max {high_5min}, 15min max {high_15min}")
                
#                 if consolidation_start is not None and (df.index[i] - consolidation_start).total_seconds() / 60 >= 15:
#                     close_below_vwap_count = (df['close'].iloc[i-14:i+1] < df['vwap'].iloc[i-14:i+1]).sum()
                    
#                     logging.info(f"Consolidation period reached at index {i}, duration: {(df.index[i] - consolidation_start).total_seconds() / 60} minutes, VWAP count: {close_below_vwap_count}")
                    
#                     if close_below_vwap_count <= 5 and current_high > breakout_level:
#                         logging.info(f"Breakout confirmed at index {i}, breakout level: {breakout_level}")
#                         return max_price, max_time
#                     else:
#                         logging.info(f"Consolidation violated at index {i}, VWAP count: {close_below_vwap_count}, breakout level: {breakout_level}")
#                         consolidation_start = None
#                         breakout_level = None

#         if max_price and not reset_threshold:
#             return max_price, max_time
#         else:
#             return overall_max_price, overall_max_time

#     logging.warning("No data found to determine max price or reset condition met.")
#     return None, None

# async def check_consolidation(contract, max_price, max_time):
#     start_time = make_offset_aware(max_time, common_timezone) - timedelta(minutes=CONSOLIDATION_TIME)
#     end_time = make_offset_aware(datetime.now(), common_timezone)
#     duration = f'{(end_time - start_time).total_seconds()} S'
#     bars = await ib.reqHistoricalDataAsync(
#         contract,
#         endDateTime=end_time.strftime('%Y%m%d %H:%M:%S'),
#         durationStr=duration,
#         barSizeSetting='1 min',
#         whatToShow='TRADES',
#         useRTH=True
#     )
#     if bars:
#         df = pd.DataFrame(
#             [(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume) for bar in bars],
#             columns=['datetime', 'open', 'high', 'low', 'close', 'volume']
#         )
#         df.set_index(pd.DatetimeIndex(df['datetime']), inplace=True)
#         vwap = ta.vwap(df['high'], df['low'], df['close'], df['volume'])
#         closes_below_vwap = (df['close'] < vwap).sum()
#         result = closes_below_vwap <= MAX_CLOSE_BELOW_VWAP
#         logging.info(f"Consolidation check result: {result} with {closes_below_vwap} closes below VWAP")
#         return result
#     logging.warning("No data found for consolidation check.")
#     return False

async def place_buy_order(contract, quantity):
    order = MarketOrder('BUY', quantity)
    trade = ib.placeOrder(contract, order)
    logging.info(f"Buy order placed for quantity: {quantity}")
    return trade

async def place_stop_loss_order(contract, quantity, stop_price):
    order = StopOrder('SELL', quantity, stop_price)
    trade = ib.placeOrder(contract, order)
    logging.info(f"Stop loss order placed at price: {stop_price}")
    return trade

async def check_bearish_crossover(contract):
    bars = await ib.reqHistoricalDataAsync(contract, '', '1 D', '3 mins', 'TRADES', useRTH=True)
    if bars:
        sma10 = np.mean([bar.close for bar in bars[-10:]])
        sma30 = np.mean([bar.close for bar in bars[-30:]])
        result = sma10 < sma30
        logging.info(f"Bearish crossover check result: {result}")
        return result
    logging.warning("No data found for bearish crossover check.")
    return False

async def close_position(contract):
    positions = ib.positions()
    for position in positions:
        if position.contract == contract and position.position > 0:
            order = MarketOrder('SELL', abs(position.position))
            trade = ib.placeOrder(contract, order)
            logging.info("Position closed.")
            return trade
    logging.info("No position to close.")
    return None

async def run_strategy(contract):
    first_trade_profitable = None
    second_trade_entered = False
    max_prices = []
    max_times = []
    ctrct = Stock(symbol='SPY', exchange='ARCA', currency='USD')
    await ib.qualifyContractsAsync(ctrct)
    while True:
        current_time = await ib.reqCurrentTimeAsync()
        logging.info(f"Current Time: {current_time}")
        if not await check_sma_slopes(contract):
            logging.info("SMA slopes are not positive. Sleeping for 60 seconds.")
            await asyncio.sleep(60)
            continue
        # new_max_prices, new_max_times = await find_max_price(contract, ORB_LENGTH)
        # new_max_prices, new_max_times = await find_max_price(contract, 30, 5, 15)
        max_price, max_time = await find_max_price(contract, ORB_LENGTH )

        if max_price and max_time:
            print(f"Maximum price: {max_price} at {max_time}")
        else:
            print("No valid breakout found.")  
        if max_price is not None and max_time is not None:
            max_prices.append(max_price)
            max_times.append(max_time)

            for max_price, max_time in zip(max_prices, max_times):
                    current_time = datetime.now().time()
                    logging.info(f"Current Time: {current_time}")
                    if current_time <= LATEST_BUY_TIME:

                        buy_trade = await place_buy_order(ctrct, 1)
                        avg = buy_trade.orderStatus.avgFillPrice
                        stop_loss_price = min(max_price * (1 - STOP_LOSS_PERCENTAGE), avg * 0.997)
                        logging.info(f"Stop loss price: {stop_loss_price}")
                        await place_stop_loss_order(ctrct, 1, stop_loss_price)
                        while True:
                            if await check_bearish_crossover(contract) or datetime.now().time() >= CLOSE_TIME:
                                close_trade = await close_position(ctrct)
                                cl_avg = close_trade.orderStatus.avgFillPrice
                                if close_trade and cl_avg> avg:
                                    first_trade_profitable = True
                                break
                            await asyncio.sleep(60)
                        break
            if not first_trade_profitable and not second_trade_entered:
                logging.info("First trade was not profitable. Checking for potential second buy.")
                for max_price, max_time in zip(max_prices, max_times):
                        current_time = datetime.now().time()
                        if current_time <= LATEST_BUY_TIME:
                            buy_trade = await place_buy_order(ctrct, 1)
                            avg = buy_trade.orderStatus.avgFillPrice
                            stop_loss_price = min(max_price * (1 - STOP_LOSS_PERCENTAGE), avg * 0.997)
                            await place_stop_loss_order(ctrct, 1, stop_loss_price)
                            second_trade_entered = True
                            while True:
                                if await check_bearish_crossover(contract) or datetime.now().time() >= CLOSE_TIME:
                                    await close_position(ctrct)
                                    break
                                await asyncio.sleep(60)
                            break
        else:
            logging.info("No maximum prices found. Sleeping for 60 seconds.")
            await asyncio.sleep(60)

        if first_trade_profitable or second_trade_entered:
            logging.info("Trading complete for today.")
            break

async def main():
    try:
        await ib.connectAsync('3.14.248.152', 7497, clientId=1)
        contract = Index(symbol='SPX', exchange='CBOE', currency='USD')
        qualified_contracts = await ib.qualifyContractsAsync(contract)
        if not qualified_contracts:
            logging.error("Failed to qualify contract.")
            return
        contract = qualified_contracts[0]
        logging.info(f"Contract qualified: {contract}")
        await run_strategy(contract)
    except Exception as e:
        logging.error(f"Error in main: {e}")
    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())





# async def find_max_price(contract, duration_minutes, orb_minutes=30, lookback_minutes=5, forward_minutes=15):
#     # Fetch data for the specified duration
#     duration = f"{duration_minutes*60} S"
#     bars = await ib.reqHistoricalDataAsync(
#         contract,
#         endDateTime='',
#         durationStr=duration,
#         barSizeSetting='1 min',
#         whatToShow='TRADES',
#         useRTH=True
#     )

#     if bars:
#         df = pd.DataFrame([(bar.date, bar.high, bar.low, bar.close, bar.volume) for bar in bars],
#                           columns=['date', 'high', 'low', 'close', 'volume'])
#         df['date'] = pd.to_datetime(df['date'])
#         df.set_index('date', inplace=True)
#         df['vwap'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])

#         # Find the high of the Opening Range Breakout (ORB)
#         orb_high = df['high'].iloc[:orb_minutes].max()
#         logging.info(f"ORB High: {orb_high}")

#         max_price = None
#         max_time = None
#         reset_threshold = False
#         consolidation_start = None
#         breakout_level = None

#         for i in range(orb_minutes, len(df) - forward_minutes):
#             if df['high'].iloc[i] > orb_high:
#                 current_high = df['high'].iloc[i]
#                 high_5min = df['high'].iloc[i-lookback_minutes:i].max()
#                 high_15min = df['high'].iloc[i+1:i+forward_minutes+1].max()

#                 if current_high > high_5min and current_high > high_15min:
#                     close_below_vwap_count = (df['close'].iloc[i+1:i+6] < df['vwap'].iloc[i+1:i+6]).sum()

#                     if close_below_vwap_count > 5:
#                         logging.info(f"Reset triggered at index {i}, high: {current_high}, VWAP reset count: {close_below_vwap_count}")
#                         reset_threshold = True
#                         max_price = None
#                         max_time = None
#                         consolidation_start = None
#                         breakout_level = None
#                     else:
#                         if max_price is None or current_high > max_price:
#                             max_price = current_high
#                             max_time = df.index[i]
#                             logging.info(f"New Max Found: {max_price} at {max_time}")
#                             consolidation_start = df.index[i]
#                             breakout_level = max_price
#                 else:
#                     if consolidation_start is not None and (df.index[i] - consolidation_start).total_seconds() / 60 >= 15:
#                         close_below_vwap_count = (df['close'].iloc[i-14:i+1] < df['vwap'].iloc[i-14:i+1]).sum()

#                         if close_below_vwap_count <= 5 and df['high'].iloc[i] > breakout_level:
#                             logging.info(f"Breakout confirmed at index {i}, breakout level: {breakout_level}")
#                             return max_price, max_time
#                         else:
#                             logging.info(f"Consolidation violated at index {i}, VWAP count: {close_below_vwap_count}, breakout level: {breakout_level}")
#                             consolidation_start = None
#                             breakout_level = None

#         if max_price and not reset_threshold:
#             return max_price, max_time

#     logging.warning("No data found to determine max price or reset condition met.")
#     return None, None
# This updated code:

# Fetches the high of the Opening Range Breakout (ORB) for the specified orb_minutes (default: 30 minutes).
# Starts the analysis from the end of the ORB period.
# Identifies the maximum price (Max_1) where the current high is higher than the last 5 minutes and the next 15 minutes.
# Checks for the reset condition (Step 1a) after Max_1 is found. If the reset condition is met, it looks for a new max (Max_2).
# Checks for the consolidation period (Step 2) under Max_1 or Max_2 for 15+ minutes, and confirms the breakout if the price climbs above the breakout level.
# During the consolidation period (Step 2a), it allows a maximum of five 1-minute candle closes below the VWAP.
# If the consolidation is violated (more than five closes below VWAP), it resets the consolidation and breakout level and continues looking for a new max (Max_3).
# Please note that this code assumes you have the necessary dependencies (ib, pd, ta) and have established a connection to the Interactive Brokers API.