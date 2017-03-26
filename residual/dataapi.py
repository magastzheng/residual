#coding=utf-8

import dbaccessor
import pandas
import datetime

#全局变量，定义需要处理的列和不需要处理的列
createDate = 'createDate'
settleDate = 'settleDate'
removeCols = ['FirstIndustryName', 'SecondIndustryName', 'IndusInnerCode','IndusCreatePrice','IndusSettlePrice']
keyCols = ['TradingDay', 'SecuCode', 'SecuAbbr', 'IndustrySecuCode_I', 'ExeClosePrice_CreateDate_Wind', 'ExeClosePrice_SettleDate_Wind']
excludeCols = ['TradingDay', 'SecuCode', 'SecuAbbr', 'IndustrySecuCode_I', 'FirstIndustryName', 'IndustrySecuCode_II', 'SecondIndustryName', 'EndDate', 'MaxEfctInfoPublDate', 'IndusInnerCode']
includeCols = ['ClosePrice','OpenPrice','HighPrice','LowPrice','ExeClosePrice','ExeOpenPrice','ExeHighPrice','ExeLowPrice','NonRestrictedShares','AFloats','TotalShares','TurnoverVolume','NonRestrictedCap','AFloatsCap','TotalCap','PE','PB','PS','PCF','DividendYield','DividendRatio','TTMIncome','GP_Margin','NP_Margin','ROA','ROE','AssetsTurnover','EquityTurnover','Cash_to_Assets','Liability_to_Assets','EquityMultiplier','CurrentRatio','Income_Growth_YOY_Comparable','NPPC_Growth_YOY_Comparable','GP_Margin_Comparable','GP_Margin_Growth_YOY_Comparable','NP_Margin_Comparable','NP_Margin_Growth_YOY_Comparable','Income_Growth_Pre_Comparable','NPPC_Growth_Pre_Comparable','GP_Margin_Growth_Pre_Comparable','NP_Margin_Growth_Pre_Comparable','NPPC_Growth_Pre_Season','NPPC_Growth_YOY_Season','NPLNRP_Growth_Pre_Season','NPLNRP_Growth_YOY_Season','Income_Growth_Pre_Season','Income_Growth_YOY_Season','Income_Growth_Qtr_Comparable','NPPC_Growth_Qtr_Comparable','GP_Margin_Qtr','GP_Margin_Growth_Qtr_Comparable','IPS_Qtr','EPS_Qtr','ROE_Qtr','Income_Growth_Pre','NPPC_Growth_Pre','NPLNRP_Growth_Pre','GP_Margin_Growth_Pre','NP_Margin_Growth_Pre','Income_Growth_YOY','NPPC_Growth_YOY','NPLNRP_Growth_YOY','GP_Margin_Growth_YOY','NP_Margin_Growth_YOY','IPS','EPS','CFPS','Pre_IPS','Pre_EPS','Pre_CFPS','YOY_IPS','YOY_EPS','YOY_CFPS','rPE','rPB','rPS','rPCF','SettlePrice','ExecutivesProp','InstitutionNum','InstitutionProp','RegionScore', 'Momentum20Day','Momentum40Day','Momentum60Day','Momentum120Day','Momentum180Day','Momentum240Day','PriceDiff','DayDiff','TurnoverRatio','AvgTurnoverPrice','AvgTurnoverPriceFactor','AvgTurnoverRatio5Day','AvgTurnoverRatio10Day','AvgTurnoverRatio20Day','AvgTurnoverRatio40Day','AvgTurnoverRatio60Day','AvgTurnoverRatio120Day', 'E_EPS', 'E_growth', 'FreeFloat_Total', 'EPS_CFPS', 'AvgTurnoverValue']


def getengine():
		host = '176.1.11.55'
		#host = 'localhost'
		user = 'zhenggq'
		password = 'Yuzhong0931'
		#password = 'yuzhong'
		return dbaccessor.getdb(host, user, password)

def getFactorDailyAllData():
		sql = '''select
				 * 
				 from baozh.dbo.FactorDaily_Comparable'''
		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

def getTradingDay():
	sql = '''select distinct convert(varchar, TradingDay, 112) as TradingDay
			 from baozh.dbo.FactorDaily_Comparable
             where TradingDay is not null
             order by TradingDay desc
		  '''
	engine = getengine()
	df = dbaccessor.query(engine, sql)
	
	return df


#获取指定那天因子数据
def getFactorDailyData(td):
		'''需要传入一个时间类型 date 或 datetime'''
		sql = '''select
				 *
				 from baozh.dbo.FactorDaily_Comparable
				 where TradingDay='{0}'
			  '''.format(td.strftime('%Y%m%d'))

		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

#获取每周交易日
def getWeeklyTradingDay():
		sql = '''select distinct convert(varchar, TradingDay, 112) as TradingDay
				from baozh.dbo.FactorTradingDay_Comparable
				where TradingDay is not null
				order by TradingDay desc
			  '''

		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

#从周数据中获取指定某交易日的数据
def getFactorWeeklyData(td):
		'''需要传入一个时间类型 date 或 datetime'''
		sql = '''select
				 *
				 from baozh.dbo.FactorTradingDay_Comparable
				 where TradingDay='{0}'
			  '''.format(td.strftime('%Y%m%d'))

		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

#获取每月交易日
def getMonthlyTradingDay():
		sql = '''select distinct convert(varchar, TradingDay, 112) as TradingDay
				from baozh.dbo.FactorMonthly_Comparable
				where TradingDay is not null
				order by TradingDay desc
			  '''

		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

#从月数据中获取指定某交易日的数据
def getFactorMonthlyData(td):
		'''需要传入一个时间类型 date 或 datetime'''
		sql = '''select
				 *
				 from baozh.dbo.FactorMonthly_Comparable
				 where TradingDay='{0}'
			  '''.format(td.strftime('%Y%m%d'))

		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

def getMonthlyStocks():
	sql = '''select distinct SecuCode
			from baozh.dbo.FactorMonthly_Comparable
			order by SecuCode
		  '''
	engine = getengine()
	df = dbaccessor.query(engine, sql)
	
	return df

#从每日数据中获取某种股票某交易日的数据
def getFactorDailyStockData(secucode):
		"""	secucode - 股票代码
		"""
		sql = '''select 
				 *
				 from baozh.dbo.FactorDaily_Comparable
				 where SecuCode='{0}'
				 order by TradingDay desc
			'''.format(secucode)
		
		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

#从周数据中获取某种股票的数据
def getFactorTradingDayStockData(secucode):
		"""	secucode - 股票代码
		"""
		sql = '''select 
				 *
				 from baozh.dbo.FactorTradingDay_Comparable
				 where SecuCode='{0}'
				 order by TradingDay desc
			'''.format(secucode)

		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

#从月数据中获取某种股票的数据
def getFactorMonthlyStockData(secucode):
		"""	secucode - 股票代码
		"""
		sql = '''select 
				 *
				 from baozh.dbo.FactorMonthly_Comparable
				 where SecuCode='{0}'
				 order by TradingDay desc
			'''.format(secucode)
		
		engine = getengine()
		df = dbaccessor.query(engine, sql)
		return df

def getFactorMonthlyDelta():
	
	sql = '''
			select * from advancedb.dbo.FactorMonthlyTopBottomDelta
			order by TradingDay desc
		'''

	engine = getengine()
	df = dbaccessor.query(engine, sql)
	return df


#获取交易日
#TradingDay type
#1 - All TraingDay
#2 - Weekly TradingDay
#3 - Monthly TradingDay
tdtypes = {
		"d": getTradingDay,
		"w": getWeeklyTradingDay,
		"m": getMonthlyTradingDay
}

#获取不同频率数据
#TradingDay type
#1 - All TraingDay
#2 - Weekly TradingDay
#3 - Monthly TradingDay
fdtypes = {
	'd': getFactorDailyData,
	'w': getFactorWeeklyData,
	'm': getFactorMonthlyData,
}

#获取不同频率数据
#TradingDay type
#1 - All TraingDay
#2 - Weekly TradingDay
#3 - Monthly TradingDay
stdtypes = {
	'd': getFactorDailyStockData,
	'w': getFactorTradingDayStockData,
	'm': getFactorMonthlyStockData,
}

deltatypes = {
	'm': getFactorMonthlyDelta,				
}

def getDayList(dtype):
		"""	获取指定类型的交易日列表
			dtype - 字符表示交易日类型，每天/每周/每月

			return - 交易日列表datetime.date
		"""
		fn = tdtypes.get(dtype, getTradingDay)
		tddf = fn()

		tddf['TradingDay'] = tddf['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))
		return tddf['TradingDay'].tolist()

def getFactorData(dtype, td):
		"""	获取指定类型指定交易日的因子数据
			dtype - 字符表示交易日类型，每天/每周/每月
			td - datetime类型的时间表示交易日

			return - pandas DataFrame类型的数据
		"""
		fn = fdtypes.get(dtype, getFactorDailyData)
		df = fn(td)

		return df

def getFactorStockData(dtype,secucode):
		"""	获取指定类型、指定股票在指定交易日的因子数据
			dtype - 字符表示交易日类型，每天/每周/每月
			secucode - 股票代码

			return - pandas DataFrame类型的数据
		"""
		fn = stdtypes.get(dtype, getFactorDailyStockData)
		df = fn(secucode)

		return df

def getFactorDelta(dtype):
		
		fn = deltatypes.get(dtype, getFactorMonthlyDelta)
		df = fn()

		return df
