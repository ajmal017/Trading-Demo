# Defaults
import time
# Imports from IB official API
from ib.ext.Order import Order
from ib.ext.Contract import Contract
from ib.opt import Connection, ibConnection, message
ACCOUNT_ID = 'Your IB Account ID'
class timing:
	def wait(self, ts):
		print('Wait to {}...'.format(ts))
		print('Current time: {}'.format(self.getCurrentTime()))
		while True:
			current_time = self.getCurrentTime()
			# print(current_time, ts)
			time.sleep(5)
			if current_time > ts:
				break
			else:
				continue
	def getCurrentTime(self):
		# get current UTC timestamp
		date = pd.Timestamp(datetime.datetime.utcnow())
		date = date.tz_localize('UTC')
		return date
	def get_schedule(self, current_date):
		previous_date = current_date - pd.Timedelta('5 days')
		future_date = current_date + pd.Timedelta('5 days')

		nyse = mcal.exchange_calendar_nyse.NYSEExchangeCalendar()

		schedule = nyse.schedule(
			start_date = previous_date,
			end_date = future_date
		)
		return schedule
	def getNextMarketOpenTs(self):
		# get next market open timestamp
		current_date = self.getCurrentTime()
		schedule = self.get_schedule(current_date)
		market_opens = schedule['market_open']
		# print(market_opens)
		market_open = market_opens[market_opens >= current_date]
		market_open = market_open.loc[market_open.index[0]]
		return market_open
	def getMarketOpenTs2days(self):
		# get next market open timestamp
		current_date = self.getCurrentTime()
		schedule = self.get_schedule(current_date)
		market_opens = schedule['market_open']
		# print(market_opens)
		market_open = market_opens[market_opens >= current_date]
		market_open = market_open.loc[market_open.index[1]]
		return market_open
	def getNextMarketCloseTs(self):
		# get next market open timestamp
		current_date = self.getCurrentTime()
		schedule = self.get_schedule(current_date)
		market_closes = schedule['market_close']
		# print(market_closes)
		market_close = market_closes[market_closes >= current_date]
		market_close = market_close.loc[market_close.index[0]]
		return market_close
	def getPreviousMarketCloseTs(self):
		# get next market close timetsamp
		current_date = self.getCurrentTime()
		schedule = self.get_schedule(current_date)
		market_closes = schedule['market_close']
		market_close = market_closes[market_closes <= current_date]
		market_close = market_close.loc[market_close.index[-1]]
		return market_close
	def getPreviousMarketOpenTs(self):
		# get next market open timetsamp
		current_date = self.getCurrentTime()
		schedule = self.get_schedule(current_date)
		market_opens = schedule['market_open']
		market_open = market_opens[market_opens <= current_date]
		market_open = market_open.loc[market_open.index[-1]]
		return market_open

class order_initialization:
	def create_contract(symbol, sec_type, exchange, primary_exchange, currency):
		contract = Contract()
		contract.m_symbol = symbol
		contract.m_secType = sec_type
		contract.m_exchange = exchange
		contract.m_primaryExch = primary_exchange
		contract.m_currency = currency
		return contract
	def create_order(order_type, qt, action, limit_price = None, tif = 'DAY'):
		order = Order()
		order.m_orderType = order_type
		order.m_totalQuantity = qt
		order.m_action = action
		order.m_tif = tif
		order.m_lmtPrice = limit_price
		return order
	def get_order_id(self):
		cwd = os.listdir()
		if 'current_order_id.csv' not in cwd:
			data = pd.DataFrame({0: 0}, index = [0])
			data.to_csv('current_order_id.csv')
		data = pd.read_csv('current_order_id.csv', index_col = 0)
		order_id = data.loc[data.index[0], data.columns[0]]
		data = pd.DataFrame({0: order_id + 1}, index = [0])
		data.to_csv('current_order_id.csv')
		return int(order_id)
def get_pending_orders():
	ib_client = ib_insync.IB()
	ib_client = ib_client.connect('127.0.0.1', 7496, clientId = 708)

	open_orders = ib_client.reqAllOpenOrders()
	ib_client.disconnect()
	return open_orders

def get_clean_portfolio(tries=20):
	num_tries = 0
	global portfolio_client_id
	while True:
		try:
			ib_client = ib_insync.IB()
			ib_client = ib_client.connect('127.0.0.1', 7496, portfolio_client_id)

			portfolio = ib_client.portfolio()
			total_portfolio = {}

			for portfolio_elem in portfolio:
				contract = portfolio_elem.contract
				symbol = contract.symbol
				position = portfolio_elem.position

				if type(contract) == ib_insync.contract.Stock:
					total_portfolio.update({symbol: position})
				elif type(contract) == ib_insync.contract.Option:
					expiration_date = contract.lastTradeDateOrContractMonth
					right = contract.right
					strike = contract.strike

					year = expiration_date[:4]
					month = expiration_date[4:6]
					day = expiration_date[6:]
					# expiration_date = pd.Timestamp('{}-{}-{}'.format(year, month, day))
					if right == 'C':
						right = 'Call'
					elif right == 'P':
						right = 'Put'
					option_contract_name = '{} {}/{}/{} {} {}'.format(symbol, year, month, day, strike, right)
					total_portfolio.update({option_contract_name: position})
			ib_client.disconnect()
			break
		except Exception as e:
			try:
				ib_client.disconnect()
			except:
				pass
			num_tries += 1
			portfolio_client_id += 1
			if num_tries > tries:
				total_portfolio = {}
				break
	return total_portfolio
class capital:
	def __init__(self):
		self.capital = 0
		self.account_id = ACCOUNT_ID
		self.q = queue.Queue()
		self.timeout = 20
	def capital_message_handler(self, msg):
		try:
			if msg.key == 'RegTEquity':
				self.capital = msg.value
		except:
			pass
	def get_capital(self):
		conn = ibConnection(port = 7496, clientId = 701)
		conn.registerAll(self.capital_message_handler)
		conn.connect()
		conn.reqAccountUpdates(True, self.account_id)
		time.sleep(10)
		conn.disconnect()
		return self.capital
def market_buy(stock, qt):
	conn = Connection.create(port = 7496, clientId = 1)
	conn.connect()

	contract = order_initialization.create_contract(stock, 'STK', 'SMART', 'SMART', 'USD')
	order = order_initialization,create_order('MKT', qt, 'BUY')
	get_order_id = order_initialization.get_order_id()
	conn.placeOrder(order_id, contract, order)
	time.sleep(5)
	conn.disconnect()

def market_sell(stock, qt):
	conn = Connection.create(port = 7496, clientId = 1)
	conn.connect()

	contract = order_initialization.create_contract(stock, 'STK', 'SMART', 'SMART', 'USD')
	order = order_initialization,create_order('MKT', qt, 'SELL')
	get_order_id = order_initialization.get_order_id()
	conn.placeOrder(order_id, contract, order)
	time.sleep(5)
	conn.disconnect()


def limit_buy(stock, qt, limit_price):
	conn = Connection.create(port = 7496, clientId = 1)
	conn.connect()

	contract = order_initialization.create_contract(stock, 'STK', 'SMART', 'SMART', 'USD')
	order = order_initialization,create_order('LMT', qt, 'BUY', limit_price)
	get_order_id = order_initialization.get_order_id()
	conn.placeOrder(order_id, contract, order, limit_price)
	time.sleep(5)
	conn.disconnect()

def limit_sell(stock, qt, limit_price):
	conn = Connection.create(port = 7496, clientId = 1)
	conn.connect()

	contract = order_initialization.create_contract(stock, 'STK', 'SMART', 'SMART', 'USD')
	order = order_initialization,create_order('LMT', qt, 'SELL', limit_price)
	get_order_id = order_initialization.get_order_id()
	conn.placeOrder(order_id, contract, order)
	time.sleep(5)
	conn.disconnect()


class data_retrieval:
	def __init__(self):
		self.primary_exchange = 'SMART'
		self.exchange = 'SMART'
		self.shortability = 0

	def get_top_movers(self):
		ib_s = ib_insync.IB()
		ib_s.connect('127.0.0.1', 7496, clientId = 710)
		allParams = ib_s.reqScannerParameters()
		# prit(allParams)
		sub = ib_insync.ScannerSubscription(
			instrument='STK',
			locationCode='STK.US.MAJOR',
			marketCapAbove=5000,
			scanCode='TOP_PERC_GAIN')
		scanData = ib_s.reqScannerData(sub)
		def scan_data(elem):
			try:
				stock = elem.contractDetails.contract.symbol
				return stock
			except Exception as e:
				print(e)
				return [np.nan, np.nan]

		datas = Parallel(-1, 'loky', verbose = 0)(delayed(scan_data)(elem) for elem in scanData)
		return datas
	def shortability_msg_handler(self, msg):
		try:
			# print(msg)
			if msg.tickType == 46:
				self.shortability = msg.value
		except:
			pass

	def get_stock_shortability(self, stock):
		global value
		value = 0
		current_id = 711
		while True:
			try:
				tws = ibConnection(port=7496, clientId=current_id)
				tws.registerAll(self.shortability_msg_handler)

				tws.connect()

				c = Contract()
				c.m_symbol = stock
				c.m_secType = 'STK'
				c.m_exchange = "SMART"
				c.m_currency = "USD"
				tws.reqMarketDataType(3)

				tws.reqMktData(46,c,"236",False)
				time.sleep(3)

				tws.disconnect()

				try:
					return self.shortability
				except:
					return np.nan
				break
			except:
				current_id += 1
