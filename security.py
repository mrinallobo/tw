import asyncio

import logging

from ib_insync import *

import pandas as pd

import csv

from datetime import datetime

import os

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(
    filename='trading_log.log',  # The name of the file to log to
    level=logging.INFO,  # The minimum level of messages to log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format of the log messages
    filemode='w'  # Mode to open the file ('w' for write/overwrite, 'a' for append)
)

ib = IB()

ib.connect('3.14.248.152', 7497, clientId=31)

async def cancel_all_open_orders():

    logging.info("Cancelling all open orders...")

    for trade in ib.openTrades():

        ib.cancelOrder(trade.order)

    logging.info("All open orders cancelled.")


async def close_open_positions():

    possy = await ib.reqPositionsAsync()

    print(possy)

    for pos in possy :

        contract = pos.contract

        if contract.secType == 'STK' :

            if pos.position > 0 :

                action = 'SELL'

                sqoff = Stock(pos.contract.symbol,"SMART",pos.contract.currency)

                await ib.qualifyContractsAsync(sqoff)

            elif pos.position < 0 :

                action = 'BUY'

                sqoff = Stock(pos.contract.symbol,"SMART",pos.contract.currency)

                await ib.qualifyContractsAsync(sqoff)

        elif contract.secType == 'OPT':

            if pos.position > 0 :

                action = 'SELL'

                sqoff = Option(pos.contract.symbol,pos.contract.lastTradeDateOrContractMonth,pos.contract.strike,pos.contract.right,"SMART",pos.contract.multiplier,pos.contract.currency)

                await ib.qualifyContractsAsync(sqoff)

            elif pos.position < 0 :

                action = 'BUY'

                sqoff = Option(pos.contract.symbol,pos.contract.lastTradeDateOrContractMonth,pos.contract.strike,pos.contract.right,"SMART",pos.contract.multiplier,pos.contract.currency)

                await ib.qualifyContractsAsync(sqoff)

        if pos.position != 0 :

            Order = MarketOrder(action, abs(pos.position))

            ib.placeOrder(sqoff, Order)

        else :

            print("No positions")

            return



async def get_spx_value():

    spx = Index('SPX', 'CBOE')


    ib.reqMarketDataType(1)

    [ticker] = await ib.reqTickersAsync(spx)

    spxValue = ticker.marketPrice()

    print(spxValue)

    # spxValue = 5070

    return spxValue



async def get_option_chains( ):

    spx = Index('SPX', 'CBOE')

    await ib.qualifyContractsAsync(spx)

    logging.info("Getting option chains...")

    chains = await ib.reqSecDefOptParamsAsync(spx.symbol, '', spx.secType, spx.conId)

    util.df(chains)

    chain = next(c for c in chains if c.tradingClass == 'SPXW' and c.exchange == 'SMART')

    logging.info("Option chains retrieved.")

    return chain


async def process_options(chain, spxValue):

    while True:

        strikes = [strike for strike in chain.strikes

                   if strike % 5 == 0

                   and spxValue - 200 < strike < spxValue + 200]

        expirations = sorted(exp for exp in chain.expirations)[:3]
        rights = ['P', 'C']

        desired_expiration = expirations[0]
        print(desired_expiration)  # Change this to the desired expiration date

        contracts = [Option('SPX', desired_expiration, strike, right, 'SMART',tradingClass='SPXW')
                     for right in rights
                     for strike in strikes]
        return contracts
        # contracts = await ib.qualifyContractsAsync(*contracts)
        # print(len(contracts))
        # if contracts:

        #     return contracts

        # else:

        #     print("No valid options found. Retrying...")

        #     await asyncio.sleep(1)

async def choose_strikes(contracts):

    while True:
        
        # print(contracts)
        tickers = await ib.reqTickersAsync(*contracts)
        print("Tickers are ")
        print(tickers)

        call_deltas = [0.05, 0.1]

        put_deltas = [-0.05, -0.1]

        await asyncio.sleep(2)

        call_tickers = [ticker for ticker in tickers if ticker.contract.right == 'C' and ticker.modelGreeks and call_deltas[0] < ticker.modelGreeks.delta < call_deltas[1]]
        print("Call tickers")
        print(call_tickers)
        put_tickers = [ticker for ticker in tickers if ticker.contract.right == 'P' and ticker.modelGreeks and put_deltas[0] > ticker.modelGreeks.delta > put_deltas[1]]
        print("put tickers")
        print(put_tickers)
        max_ltp_call = max(call_tickers, key=lambda ticker: ticker.last, default=None)

        max_ltp_put = max(put_tickers, key=lambda ticker: ticker.last, default=None)

        if max_ltp_call and max_ltp_put:

            return max_ltp_call, max_ltp_put

        else:

            print("No valid call and put strikes found. Retrying...")

            await asyncio.sleep(1)


async def qualify_contracts(max_ltp_put,max_ltp_call):

    short_contracts = [max_ltp_put.contract, max_ltp_call.contract]

    await ib.qualifyContractsAsync(*short_contracts)

    return max_ltp_put.contract,max_ltp_call.contract


async def create_and_qualify_contract(short_contract, strike_offset, right):

    long_contract = Contract()

    long_contract.symbol = short_contract.symbol

    long_contract.secType = short_contract.secType

    long_contract.lastTradeDateOrContractMonth = short_contract.lastTradeDateOrContractMonth

    long_contract.strike = short_contract.strike - strike_offset

    long_contract.right = right

    long_contract.exchange = short_contract.exchange

    await ib.qualifyContractsAsync(long_contract)

    return long_contract


def get_contract_data(contract):

    md = ib.reqTickers(contract)[0]

    greeks = md.modelGreeks

    return {'Timestamp': datetime.now(), 'Contract': contract.localSymbol, 'Strike': contract.strike,

            'Delta': greeks.delta, 'Vega': greeks.vega, 'Gamma': greeks.gamma, 'Theta': greeks.theta, 'IV': greeks.impliedVol}


async def write_csv_greeks(contracts_data):

    csv_file_path = 'greeks_data.csv'

    with open(csv_file_path, 'a', newline='') as csv_file:

        fieldnames = ['Timestamp', 'Contract', 'Strike', 'Delta', 'Vega', 'Gamma', 'Theta', 'IV']

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()

        writer.writerows(contracts_data)



async def create_contract(short_contract, long_contract, action):

    return Contract(

        secType='BAG',

        symbol='SPX',

        exchange='SMART',

        currency='USD',

        comboLegs=[

            ComboLeg(

                conId=short_contract.conId,

                ratio=1,

                action='SELL',

                exchange='SMART'),

            ComboLeg(

                conId=long_contract.conId,

                ratio=1,

                action=action,

                exchange='SMART'),

        ])



async def place_order(contract, order_ref, midpoint_price):

    order = LimitOrder(action='BUY', totalQuantity=1, orderRef=order_ref, lmtPrice=midpoint_price)
    trade = ib.placeOrder(contract, order)
    return trade

async def get_fills(instrument):

    while True:

        avg_fill_price = instrument.orderStatus.avgFillPrice
        print(avg_fill_price)
        if avg_fill_price != 0.0:

            return avg_fill_price

        else:

            await asyncio.sleep(1)


async def place_market_order_if_not_filled(contract, order):

    ib.placeOrder(contract, order)

    await asyncio.sleep(5)  # Wait for 5 seconds
    if order.orderStatus.status != 'Filled':

        print(f"Order {order.orderRef} not filled within 5 seconds. Placing market order.")

        market_order = MarketOrder(action=order.action, totalQuantity=order.totalQuantity, orderRef=order.orderRef)

        ib.placeOrder(contract, market_order)


async def initiate_new_trade():

    spxValue = ib.run(get_spx_value())

    chain = ib.run(get_option_chains())

    options = ib.run(process_options(chain, spxValue))

    call, put = ib.run(choose_strikes(options))

    short_put, short_call = ib.run(qualify_contracts(call, put))

    offset = 30

    long_call = ib.run(create_and_qualify_contract(short_call, offset, short_call.right))

    long_put = ib.run(create_and_qualify_contract(short_put, -offset, short_put.right))

    bear_call = ib.run(create_contract(short_call, long_call, 'BUY'))

    bull_put = ib.run(create_contract(short_put, long_put, 'BUY'))

    bc_avg = (call.bid + call.ask) / 2

    bp_avg = (put.bid + put.ask) / 2

    return bear_call, bull_put, bc_avg, bp_avg




async def monitor(bear_call, bull_put, terminate_calls, terminate_puts, sqoff_calls, sqoff_puts, bull_fill, bear_fill, max_retries=3):

    contracts = [bear_call, bull_put]

    bc = contracts[0]

    bp = contracts[1]

    await ib.qualifyContractsAsync(*contracts)

    for contract in contracts:

        ib.reqMktData(contract, '', False, False)

    call = ib.ticker(bc)

    put = ib.ticker(bp)

    await asyncio.sleep(2)

    call_flag = False

    put_flag = False

    call_retries = 0

    put_retries = 0


    while not (call_flag and put_flag):

        if call_flag == False and put_flag == False:

            print(f"Net Premia is {abs(call.marketPrice() + put.marketPrice())}")

        elif call_flag == False:

            print(f"Net Premia is {call.marketPrice()}")

        elif put_flag == False:

            print(f"Net Premia is {put.marketPrice()}")


        if call.marketPrice() <= -0.01 and not call_flag:

            ct = ib.placeOrder(bear_call, terminate_calls)

            call_flag = True

            ib.cancelMktData(call)


        if put.marketPrice() <= -0.01 and not put_flag:

            ct = ib.placeOrder(bull_put, terminate_puts)

            put_flag = True

            ib.cancelMktData(put)


        if bull_fill*2 - 0.05 > call.marketPrice() and not call_flag and call_retries < max_retries:

            print("Call side SL hit")

            ct = sqoff_calls

            asyncio.create_task(place_market_order_if_not_filled(bear_call, ct))

            call_flag = True

            ib.cancelMktData(call)

            await asyncio.sleep(1)

            call_retries += 1

            if call_retries < max_retries:

                print(f"Retrying call side trade (retry {call_retries} of {max_retries})")

                bear_call, bull_put, bc_avg, bp_avg = await initiate_new_trade()

                bull_fill = bc_avg

                call_flag = False

                ib.reqMktData(bear_call, '', False, False)


        if bear_fill*2 - 0.05 > put.marketPrice() and not put_flag and put_retries < max_retries:

            print("Put side SL hit")

            cp = sqoff_puts

            asyncio.create_task(place_market_order_if_not_filled(bull_put, cp))

            put_flag = True

            ib.cancelMktData(put)

            await asyncio.sleep(1)

            put_retries += 1

            if put_retries < max_retries:

                print(f"Retrying put side trade (retry {put_retries} of {max_retries})")

                bear_call, bull_put, bc_avg, bp_avg = await initiate_new_trade()

                bear_fill = bp_avg

                put_flag = False

                ib.reqMktData(bull_put, '', False, False)


    print("Loop broken")

    ce = await get_fills(ct)

    pe = await get_fills(cp)

    return ce, pe



async def log_to_csv(contract_names, prices, action):

    if len(contract_names) != len(prices):

        print("Error: contract_names and prices lists must be of the same length.")

        return


    filename='log.csv'

    with open(filename, 'a', newline='') as file:

        writer = csv.writer(file)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for contract_name, price in zip(contract_names, prices):

            writer.writerow([timestamp, contract_name, price, action])


# async def wait_for_orders_to_fill():

#     while True:

#         open_orders = ib.openOrders()

#         if not open_orders:

#             print("All orders are filled.")

#             return

#         else:

#             print(f"Waiting for {len(open_orders)} order(s) to fill...")

#             for order in open_orders:

#                 print(f"Open order: {order.orderRef} - {order.orderType} {order.action} {order.totalQuantity} ")

#             await asyncio.sleep(1)


async def wait_for_orders_to_fill():
    import time
    start_time = time.time()
    timeout = 30  # Timeout in seconds

    while True:
        # Fetch open trades (contains order and contract info)
        open_trades = ib.openTrades()
        await asyncio.sleep(0.1)

        if not open_trades:
            print("All orders are filled.")
            return
        else:
            elapsed_time = time.time() - start_time

            if elapsed_time > timeout:
                print(f"Timeout reached. Modifying {len(open_trades)} order(s) to market orders.")
                for trade in open_trades:
                    order = trade.order
                    contract = trade.contract

                    print(f"Modifying order: {order.orderRef} - {order.orderType} {order.action} {order.totalQuantity}")
                    
                    # Create a new market order with the same action and quantity
                    market_order = Order(
                        action=order.action,
                        orderType='MKT',  # Change to market order
                        totalQuantity=order.totalQuantity
                    )

                    # Place the market order with the existing contract
                    
                    ib.placeOrder(contract, market_order)
                    
                    print(f"Modified order: {order.orderRef} - Market {order.action} {order.totalQuantity}")
                await cancel_all_open_orders()
                return
            else:
                print(f"Waiting for {len(open_trades)} order(s) to fill...")
                for trade in open_trades:
                    order = trade.order
                    print(f"Open order: {order.orderRef} - {order.orderType} {order.action} {order.totalQuantity}")

            # Wait briefly before checking again
            await asyncio.sleep(1)


logging.info("Starting script...")


def write_pnl_to_csv(entry_prices, exit_prices, contract_names):

    pnl = sum(exit_prices) - sum(entry_prices)

    current_date = datetime.now().strftime('%Y-%m-%d')

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    filename = f"pnl_report_{current_date}_{timestamp}.csv"

    
    # Create the directory if it doesn't exist

    os.makedirs('pnl_reports', exist_ok=True)


    # Open the CSV file in write mode

    with open(os.path.join('pnl_reports', filename), mode='w', newline='') as file:

        writer = csv.writer(file)


        # Write the headers

        writer.writerow(['Timestamp', 'Contract', 'Entry Price', 'Exit Price', 'PNL'])


        # Write the data rows

        for i in range(len(contract_names)):

            writer.writerow([timestamp, contract_names[i], entry_prices[i], exit_prices[i], ''])


        # Write the total PNL

        writer.writerow(['', '', '', 'Total PNL', pnl])


    print(f"PNL report generated: {filename}")


async def get_latest_call_put_fill_prices():
    try:
        # Create an execution filter to get recent executions
        exec_filter = ExecutionFilter(clientId=31)

        # Fetch recent executions
        executions = ib.reqExecutions(exec_filter)

        # Ensure there are at least two executions
        if len(executions) < 2:
            raise ValueError("Not enough executions found.")

        # Get the last two executions (latest ones)
        last_two_executions = executions[-2:]

        # Initialize call and put execution variables
        call_execution = None
        put_execution = None

        # Identify which is the call and which is the put
        for execution in last_two_executions:
            if execution.contract.secType == 'OPT' and 'C' in execution.contract.right:
                call_execution = execution
            elif execution.contract.secType == 'OPT' and 'P' in execution.contract.right:
                put_execution = execution

        # Get the fill prices of the call and put executions
        fill_prices = []
        if call_execution:
            fill_prices.append(call_execution.price)
        if put_execution:
            fill_prices.append(put_execution.price)

        return fill_prices  # Returns a list of fill prices for call and put options

    except Exception as e:
        print("An error occurred:", e)

ib.run(cancel_all_open_orders())

ib.run(close_open_positions())

spxValue =  ib.run(get_spx_value())

chain = ib.run(get_option_chains())

print(len(chain))

options = ib.run(process_options(chain, spxValue))

call, put = ib.run(choose_strikes(options))

print(f"{call.contract.strike} is short leg\n")

print(f"{put.contract.strike} is short put leg\n")

logging.info("Short strikes selected")

short_put,short_call =  ib.run(qualify_contracts(call,put))

offset = 30

long_call = ib.run(create_and_qualify_contract(short_call,offset,short_call.right))

long_put = ib.run(create_and_qualify_contract(short_put,-offset,short_put.right))

ib.run( ib.qualifyContractsAsync(long_call,long_put))

print("All strikes qualified")

contracts = [short_put, short_call, long_put, long_call]

contracts_data = ([get_contract_data(contract) for contract in contracts])

print(contracts_data)

ib.run(write_csv_greeks(contracts_data))

print("Greeks Logged")

bear_call = ib.run(create_contract(short_call, long_call, 'BUY'))
logging.info("Bear call contract created",bear_call.conId)

bull_put = ib.run(create_contract(short_put, long_put, 'BUY'))
logging.info("Bull put contract created",bull_put.conId)
ib.reqMktData(bear_call)

ib.reqMktData(bull_put)

ib.sleep(1)  # Wait for 1 second to receive market data
print(call.bid,call.ask,put.bid,put.ask)
if call.ask - call.bid == 0.05:
    price_call = -call.bid
else:
    price_call = -round((call.bid + call.ask) / 2 / 0.05) * 0.05
    
if put.ask - put.bid == 0.05:
    price_put = -put.bid
else:
    price_put = -round((put.bid + put.ask) / 2 / 0.05) * 0.05

print(price_call,price_put)    
# trade_call = ib.run(place_order(bear_call, 'BearCall', price_call))

# trade_put = ib.run(place_order(bull_put, 'BullPut', price_put))

# print("Orders placed.")

# logging.info("Orders placed")

# ib.run(wait_for_orders_to_fill())
# print("reach here?")
# # bc_avg =  ib.run(get_fills(trade_call))
# bc_avg = trade_call.orderStatus.avgFillPrice
# print(bc_avg,"Bear call average")
# # bp_avg = ib.run(get_fills(trade_put))
# bp_avg = trade_put.orderStatus.avgFillPrice
# print(bp_avg,"Bull put avg")
# print(bc_avg,"Is bull call avg\n")
# ib.reqExecutionsAsync
# print(bp_avg,"Is bull put avg")
trade_call = ib.run(place_order(bear_call, 'BearCall_Order', price_call))
trade_put = ib.run(place_order(bull_put, 'BullPut_Order', price_put))
complete_filled_orders = ib.run( wait_for_orders_to_fill())
# Retrieve all fills

ib.sleep(5)
fills = ib.fills()
# fills = ib.fills()
# print(fills)

# # Create placeholders for Call and Put fills
# call_fill = None
# put_fill = None

# # Loop through fills to find the latest Call and Put fills
# for fill in fills:
#     if fill.contract.right == 'C':  # Call contract
#         if not call_fill or fill.time > call_fill.time:
#             call_fill = fill
#     elif fill.contract.right == 'P':  # Put contract
#         if not put_fill or fill.time > put_fill.time:
#             put_fill = fill

# Extract fill information for the latest Call and Put fills
# if call_fill:
#     call_avg = call_fill.execution.avgPrice
#     print(f"Latest Call Fill: Time - {call_fill.time}, Avg Price - {call_avg}")
# else:
#     print("No Call fill found")

# if put_fill:
#     put_avg = put_fill.execution.avgPrice
#     print(f"Latest Put Fill: Time - {put_fill.time}, Avg Price - {put_avg}")
# else:
#     print("No Put fill found")

ib.sleep(5)

bag_fills = [
    fill for fill in fills
    if fill.contract.secType == 'BAG'  # Assuming secType denotes the type of contract
]

# Sort these bag fills by time to get the latest and slice the first two
sorted_bag_fills = sorted(bag_fills, key=lambda x: x.time, reverse=True)[:2]

# Extract details from the sorted bag fills
for bag_fill in sorted_bag_fills:
    print("contract is ",bag_fill.contract)
    print("Contract ID:", bag_fill.contract.conId)
    print("Symbol:", bag_fill.contract.symbol)
    print("Exchange:", bag_fill.contract.exchange)
    print("Combo Legs Description:", bag_fill.contract.comboLegsDescrip)
    for leg in bag_fill.contract.comboLegs:
        print("    Leg ConID:", leg.conId)
        print("    Action:", leg.action)
        print("    Ratio:", leg.ratio)
    print("Execution Time:", bag_fill.execution.time)
    print("Price:", bag_fill.execution.price)
    print("Side:", bag_fill.execution.side)
    print("-----")


contract1 = None
contract2 = None

# Assign the contracts if available
if len(sorted_bag_fills) > 0:
    contract1 = sorted_bag_fills[0].contract
    fill1 = sorted_bag_fills[0].execution.price
    print(contract1)
    if len(sorted_bag_fills) > 1:
        contract2 = sorted_bag_fills[1].contract 
        fill2 = sorted_bag_fills[1].execution.price
        print(contract2)   




bc_avg = fill1
bp_avg = fill2


ib.run( log_to_csv([bear_call,bull_put],[bc_avg,bp_avg],"Entry"))

sqoff_calls = LimitOrder(action = 'SELL',totalQuantity = 1,orderRef='Bear call close',lmtPrice = 2*bc_avg - 0.15)

sqoff_puts = LimitOrder(action = 'SELL',totalQuantity = 1,orderRef='Bull put close',lmtPrice = 2*bp_avg - 0.15)

terminate_calls = MarketOrder(action = 'SELL',totalQuantity = 1,orderRef='Bear call terminate')

terminate_puts = MarketOrder(action = 'SELL',totalQuantity = 1,orderRef='Bull put terminate')

net_premia = bc_avg + bp_avg

print(f"{net_premia} is the net premium received")

ce,pe = ib.run( monitor(bear_call,bull_put,terminate_calls,terminate_puts,sqoff_calls,sqoff_puts,bc_avg,bp_avg))

print("Trade complete")

ib.run( write_csv_greeks(contracts_data))

ib.run(log_to_csv([bear_call,bull_put],[ce,pe],"Exit"))

ib.run(cancel_all_open_orders())

ib.run(close_open_positions())

write_pnl_to_csv([bc_avg, bp_avg], [ce, pe], [bear_call.symbol, bull_put.symbol])

logging.info("Script completed.")



# if __name__ == "__main__":

#     asyncio.run(main())