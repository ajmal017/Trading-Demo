import os
import copy
import time
import queue
import CustomIBAPI
import talib as ta
from joblib import Parallel, delayed
from iexfinance.stocks import get_historical_intraday

while True:
    market_open = CustomIBAPI.timing().getNextMarketOpenTs()
    CustomIBAPI.timing().wait(market_open - pd.Timedelta('2 minutes'))
    top_movers = CustomIBAPI.get_top_movers()
    top_movers = pd.DataFrame({'movers': top_movers})
    shortability = []
    for i in top_movers.index:
        stock = top_movers.loc[i, 'movers']
        shortability_of_stock = CustomIBAPI.get_stock_shortability(stock)
        shortability.append(shortability_of_stock)
    top_movers['shortability'] = shortability
    top_movers = top_movers[top_movers['shortability'] == 3]
    if len(top_movers) > 0:
        capital = CustomIBAPI.get_capital()
        qt = capital / len(top_movers)
        for stock in top_movers['movers']:
            CustomIBAPI.sell(stock, qt)
        while True:

		current_orders = CustomIBAPI.get_pending_orders()
		if len(current_orders) < 1:
			break

		
		current_designated_time = IBAPI_simplified_interface.timing().getCurrentTime()
		current_designated_time = current_designated_time + pd.Timedelta('1 minute')
		current_designated_time = pd.Timestamp(year = current_designated_time.year,
											   month = current_designated_time.month,
											   day = current_designated_time.day,
											   hour = current_designated_time.hour,
											   minute = current_designated_time.minute,
											   tz = 'UTC')
		while True:
			current_portfolio = CustomIBAPI.get_clean_portfolio2()
			current_timestamp = CustomIBAPI.timing().getCurrentTime()

			if len(current_portfolio) < 1:
				break
			else:
				if current_timestamp > current_designated_time:
					current_designated_time = current_timestamp + pd.Timedelta('1 minute')
					current_designated_time = pd.Timestamp(year = current_designated_time.year,
														   month = current_designated_time.month,
														   day = current_designated_time.day,
														   hour = current_designated_time.hour,
														   minute = current_designated_time.minute,
														   tz = 'UTC')
					for stock in list(current_portfolio.keys()):
						entry_price = entry_prices[stock]
						previous_timestamp = CustomIBAPI.timing().getPreviousPreviousMarketOpenTs().tz_convert('America/New_York')
						current_timestamp = CustomIBAPI.timing().getPreviousMarketOpenTs().tz_convert('America/New_York')

						datapoints_0 = get_historical_intraday(current_stock, date = previous_timestamp.date(), token = sec)
						datapoints_0 = pd.DataFrame(datapoints_0)
						datapoints_1 = get_historical_intraday(current_stock, date = current_timestamp.date(), token = sec)
						datapoints_1 = pd.DataFrame(datapoints_1)
						dpts = datapoints_0.append(datapoints_1)['open'].ffill().values

						upperbb, ___, lowerbb = ta.BBANDS(dpts, 50)
						prev_upperbb, prev_lowerbb = upperbb[-2], lowerbb[-2]
						upperbb, lowerbb = upperbb[-1], lowerbb[-1]

						if dpts[-2] < prev_upperbb:
							if dpts[-1] > upperbb or dpts[-1] < lowerbb:
								qt = current_portfolio[stock]
								qt = int(qt)
								stock_price = si.get_live_price(stock_price)
								if qt > 0:
									CustomIBAPI.orders().sell(current_stock, qt, stock_price)
								elif qt < 0:
									CustomIBAPI.orders().buy(current_stock, qt, stock_price)

    else:
        continue
