import websocket
import json
import threading
import time
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import REST, TimeFrame
from datetime import datetime, timedelta


endpoint  = "https://data.alpaca.markets/v2"
headers   = json.loads(open("./Key/alpaca_key.json",'r').read())
api       = tradeapi.REST(headers["APCA-API-KEY-ID"], 
                          headers["APCA-API-SECRET-KEY"], 
                          base_url='https://paper-api.alpaca.markets')
watchList = ['FISV','AMZN','INTC','MSFT','AAPL',
             'GOOG','CSCO','NVDA','NFLX','PYPL','QCOM']

lastTickPrice = {} 
prevClose     = {}  
percenChange  = {} 
filledOrders  = [] 
BUY_LIMIT          = 10000 
PERCENT_UP_LIMIT   = 1.1
TRAIL_PERCENT      = "1.5"
WAIT_SECOND        = 5

def historicalData(symbols, start_date, timeframe):
    
    df_data = {}
    api = REST(headers["APCA-API-KEY-ID"], headers["APCA-API-SECRET-KEY"], base_url=endpoint)
    
    for ticker in symbols:
        if timeframe == "Minute":
            df_data[ticker] = api.get_bars(ticker, TimeFrame.Minute, start_date, adjustment='all').df
        elif timeframe == "Hour":
            df_data[ticker] = api.get_bars(ticker, TimeFrame.Hour, start_date, adjustment='all').df
        else:
            df_data[ticker] = api.get_bars(ticker, TimeFrame.Day, start_date, adjustment='all').df
    return df_data

def on_open(ws):
    auth = {"action": "auth", "key": headers["APCA-API-KEY-ID"], "secret": headers["APCA-API-SECRET-KEY"]}
    ws.send(json.dumps(auth))
    message = {"action":"subscribe","trades":watchList}
    ws.send(json.dumps(message))
 
def on_message(ws, message):
    tick = json.loads(message)
    tkr = tick[0]["S"]
    lastTickPrice[tkr] = float(tick[0]["p"])
    percenChange[tkr] = round((lastTickPrice[tkr]/prevClose[tkr] - 1)*100,2) 
    if percenChange[tkr] > 2  or percenChange[tkr] < -2:
        print("--->"+tkr+ " "+percenChange[tkr])  
    

def connectToAlpaca():
    ws = websocket.WebSocketApp("wss://stream.data.alpaca.markets/v2/iex", on_open=on_open, on_message=on_message)
    ws.run_forever()

def posSize(ticker):
    return max(1,int(BUY_LIMIT/lastTickPrice[ticker]))

def scan():
    for ticker, pc in percenChange.items():
        if pc > PERCENT_UP_LIMIT  and ticker not in filledOrders:
            trade(ticker, posSize(ticker), "buy", "market", "ioc",
                                           "sell", "trailing_stop", "day")
        if pc < -1 * PERCENT_UP_LIMIT and ticker not in filledOrders:
            trade(ticker, posSize(ticker), "sell", "market", "ioc",
                                           "buy", "trailing_stop", "day")
            
def trade(symbol,qty,side,type,time_in_force,second_leg_side,second_leg_type,second_leg_time_in_force):
    '''
    api.submit_order(
    symbol='AAPL',
    qty=1,
    side='buy',
    type='market',
    time_in_force='gtc')
    '''
    api.submit_order(symbol, qty, side, type, time_in_force)
    time.sleep(WAIT_SECOND)
    try:
        filled_qty = api.get_position(symbol).qty
        time.sleep(WAIT_SECOND)
        if side == "buy":
            filled = int(filled_qty)
        if side == "sell":
            filled = -1*int(filled_qty)
        api.submit_order(symbol, filled, second_leg_side, second_leg_type, second_leg_time_in_force, trail_percent = TRAIL_PERCENT)
        filledOrders.append(symbol)
    except Exception as e:
        print(ticker, e)


## Initialize
yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
print(yesterday)
history = historicalData(watchList, yesterday, timeframe="Day")

for ticker in watchList:
    prevClose[ticker]     = history[ticker]["close"][-2]
    lastTickPrice[ticker] = history[ticker]["close"][-1]
    percenChange[ticker]  = 0
 
 

## Connect to market and get real-time stock prices till main times out
t = threading.Thread(target=connectToAlpaca, daemon=True)
t.start()

starttime = time.time()
timeout = starttime + 180*60   ## minutes
print("timeout: {}".format(timeout))
while time.time() <= timeout:
    for ticker in watchList:
        print("{} % change = {}".format(ticker,percenChange[ticker]))
        scan()
    w = 60 - ((time.time() - starttime) % 60)
    print("---")    
    time.sleep(5)

# Close all positions and cancell all orders at the end of the strategy  
api.cancel_all_orders()
api.close_all_positions()
time.sleep(WAIT_SECOND)
