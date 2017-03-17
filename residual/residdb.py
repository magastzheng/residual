#coding: utf8

#from sqlalchemy.orm import sessionmaker
#from sqlalchemy.sql import table, column, select, update, insert
from sqlalchemy.sql import select, update, insert
from sqlalchemy.orm import Session
from sqlalchemy import (
				MetaData, 
				Table, 
				Column,
				Integer,
				Numeric,
				String,
				DateTime,
				ForeignKey
				)
#import pymssql
import dbaccessor
import pandas

def testSession():
		engine = dbaccessor.getdb('localhost', 'zhenggq', 'Yuzhong0931', 'test', 1433)
		metadata = MetaData(bind = engine)
		session = Session(bind = engine)
		
		mytable = Table('account', metadata, autoload=True)

		#select
		s =  mytable.select()
		result = session.execute(s)
		out = result.fetchall()
		print(result.keys())
		print(out)

		#insert 
		"""i = insert(mytable)
		i = i.values({"Id": 14, 'Name': 'Youare', 'Email': 'youare@youare.com', 'OpType': 1, 'PassCode': 'Youarewelcome'})

		try:
			session.execute(i)
			session.commit()
		except:
			session.rollback()
		finally:
			session.close()
		"""
		session.close()

		print('Finish')

def testInsert():
	engine = dbaccessor.getdb('localhost', 'zhenggq', 'Yuzhong0931', 'test', 1433)
	conn = engine.connect()
	metadata = MetaData()

	account = Table('account', metadata,
					Column('Id', Integer(), nullable=False),
					Column('Name', String(50), nullable=False),
					Column('Email', String(50)),
					Column('OpType', Integer()),
					Column('PassCode', String(50))
					#Column('Addition', Numeric(12, 2))
					)

	ins = account.insert().values(
					Id = 15,
					Name = 'Sqlalchemy',
					Email = 'Sqlalchemy@sqlalchemy.org',
					OpType = 1,
					PassCode = 'Sqlalchemy'
					)

	print(str(ins))

	result = conn.execute(ins)

	print(result.inserted_primary_key)

def insert(conn, tb, df):
	"""	往指定表中插入数据
		conn - 数据库连接对象
		tb - 数据库表，sqlalchemy.Table类型
		df - pandas DataFrame对象，存放需要插入的所有数据
	"""
	ins = tb.insert()
	dataList = df.T.to_dict().values()
	try:
		result = conn.execute(ins, dataList)
	finally:
		conn.close()

def insertDaily(conn, df):
	"""	插入每日因子残差数据表
	"""
	metadata = MetaData()
	residtb = Table('FactorDailyResidual', metadata,
					Column('TradingDay', DateTime(), nullable=False),
					Column('SecuCode', String(10), nullable=False),
					Column('ClosePrice', Numeric(16, 4)),
					Column('OpenPrice', Numeric(16, 4)),
					Column('HighPrice', Numeric(16, 4)),
					Column('LowPrice', Numeric(16, 4)),
					Column('ExeClosePrice', Numeric(16, 4)),
					Column('ExeOpenPrice', Numeric(16, 4)),
					Column('ExeHighPrice', Numeric(16, 4)),
					Column('ExeLowPrice', Numeric(16, 4)),
					Column('NonRestrictedShares', Numeric(16, 0)),
					Column('AFloats', Numeric(16, 0)),
					Column('TotalShares', Numeric(16, 0)),
					Column('TurnoverVolume', Numeric(20, 0)),
					Column('NonRestrictedCap', Numeric(36, 4)),
					Column('AFloatsCap', Numeric(36, 4)),
					Column('TotalCap', Numeric(36, 4)),
					Column('SecuAbbr', String(50)),
					Column('IndustrySecuCode_I', String(10)),
					Column('PE', Numeric(38, 6)),
					Column('PB', Numeric(38, 6)),
					Column('PS', Numeric(38, 6)),
					Column('PCF', Numeric(38, 6)),
					Column('DividendYield', Numeric(16, 6)),
					Column('DividendRatio', Numeric(16, 6)),
					Column('TTMIncome', Numeric(36, 4)),
					Column('GP_Margin', Numeric(16, 4)),
					Column('NP_Margin', Numeric(16, 4)),
					Column('ROA', Numeric(16, 4)),
					Column('ROE', Numeric(16, 4)),
					Column('AssetsTurnover', Numeric(16, 4)),
					Column('EquityTurnover', Numeric(16, 4)),
					Column('Cash_to_Assets', Numeric(16, 4)),
					Column('Liability_to_Assets', Numeric(16, 4)),
					Column('EquityMultiplier', Numeric(16, 4)),
					Column('CurrentRatio', Numeric(16, 4)),
					Column('Income_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('Income_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Season', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Season', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre_Season', Numeric(32, 14)),
					Column('NPLNRP_Growth_YOY_Season', Numeric(32, 15)),
					Column('Income_Growth_Pre_Season', Numeric(16, 4)),
					Column('Income_Growth_YOY_Season', Numeric(16, 4)),
					Column('Income_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Qtr', Numeric(16, 4)),
					Column('GP_Margin_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('IPS_Qtr', Numeric(32, 21)),
					Column('EPS_Qtr', Numeric(32, 21)),
					Column('ROE_Qtr', Numeric(16, 4)),
					Column('Income_Growth_Pre', Numeric(16, 4)),
					Column('NPPC_Growth_Pre', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre', Numeric(32, 13)),
					Column('GP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('Income_Growth_YOY', Numeric(16, 4)),
					Column('NPPC_Growth_YOY', Numeric(16, 4)),
					Column('NPLNRP_Growth_YOY', Numeric(32, 13)),
					Column('GP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('IPS', Numeric(32, 21)),
					Column('EPS', Numeric(32, 21)),
					Column('CFPS', Numeric(32, 21)),
					Column('Pre_IPS', Numeric(32, 21)),
					Column('Pre_EPS', Numeric(32, 21)),
					Column('Pre_CFPS', Numeric(32, 21)),
					Column('YOY_IPS', Numeric(32, 21)),
					Column('YOY_EPS', Numeric(32, 21)),
					Column('YOY_CFPS', Numeric(32, 21)),
					Column('rPE', Numeric(32, 10)),
					Column('rPB', Numeric(32, 10)),
					Column('rPS', Numeric(32, 10)),
					Column('rPCF', Numeric(32, 10)),
					Column('SettlePrice', Numeric(16, 10)),
					Column('IndusInnerCode', Integer()),
					Column('IndusCreatePrice', Numeric(16, 10)),
					Column('IndusSettlePrice', Numeric(16, 10)),
					Column('ExecutivesProp', Numeric(16, 10)),
					Column('InstitutionNum', Integer()),
					Column('InstitutionProp', Numeric(16, 10)),
					Column('RegionScore', Integer()),
					Column('ExeClosePrice_CreateDate_Wind', Numeric(16, 4)),
					Column('ExeClosePrice_SettleDate_Wind', Numeric(16, 4)),
					Column('Momentum20Day', Numeric(20, 4)),
					Column('Momentum40Day', Numeric(20, 4)),
					Column('Momentum60Day', Numeric(20, 4)),
					Column('Momentum120Day', Numeric(20, 4)),
					Column('Momentum180Day', Numeric(20, 4)),
					Column('Momentum240Day', Numeric(20, 4)),
					Column('PriceDiff', Numeric(20, 4)),
					Column('DayDiff', Integer()),
					Column('TurnoverRatio', Numeric(20, 4)),
					Column('AvgTurnoverPrice', Numeric(20, 4)),
					Column('AvgTurnoverPriceFactor', Numeric(20, 4)),
					Column('AvgTurnoverRatio5Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio10Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio20Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio40Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio60Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio120Day', Numeric(20, 5)),
					)

	insert(conn, residtb, df)

def insertWeekly(conn, df):
	"""	插入每周因子残差数据表
	"""
	metadata = MetaData()
	residtb = Table('FactorTradingDayResidual', metadata,
					Column('TradingDay', DateTime(), nullable=False),
					Column('SecuCode', String(10), nullable=False),
					Column('ClosePrice', Numeric(16, 4)),
					Column('OpenPrice', Numeric(16, 4)),
					Column('HighPrice', Numeric(16, 4)),
					Column('LowPrice', Numeric(16, 4)),
					Column('ExeClosePrice', Numeric(16, 4)),
					Column('ExeOpenPrice', Numeric(16, 4)),
					Column('ExeHighPrice', Numeric(16, 4)),
					Column('ExeLowPrice', Numeric(16, 4)),
					Column('NonRestrictedShares', Numeric(16, 0)),
					Column('AFloats', Numeric(16, 0)),
					Column('TotalShares', Numeric(16, 0)),
					Column('TurnoverVolume', Numeric(20, 0)),
					Column('NonRestrictedCap', Numeric(36, 4)),
					Column('AFloatsCap', Numeric(36, 4)),
					Column('TotalCap', Numeric(36, 4)),
					Column('SecuAbbr', String(50)),
					Column('IndustrySecuCode_I', String(10)),
					Column('PE', Numeric(38, 6)),
					Column('PB', Numeric(38, 6)),
					Column('PS', Numeric(38, 6)),
					Column('PCF', Numeric(38, 6)),
					Column('DividendYield', Numeric(16, 6)),
					Column('DividendRatio', Numeric(16, 6)),
					Column('TTMIncome', Numeric(36, 4)),
					Column('GP_Margin', Numeric(16, 4)),
					Column('NP_Margin', Numeric(16, 4)),
					Column('ROA', Numeric(16, 4)),
					Column('ROE', Numeric(16, 4)),
					Column('AssetsTurnover', Numeric(16, 4)),
					Column('EquityTurnover', Numeric(16, 4)),
					Column('Cash_to_Assets', Numeric(16, 4)),
					Column('Liability_to_Assets', Numeric(16, 4)),
					Column('EquityMultiplier', Numeric(16, 4)),
					Column('CurrentRatio', Numeric(16, 4)),
					Column('Income_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('Income_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Season', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Season', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre_Season', Numeric(32, 14)),
					Column('NPLNRP_Growth_YOY_Season', Numeric(32, 15)),
					Column('Income_Growth_Pre_Season', Numeric(16, 4)),
					Column('Income_Growth_YOY_Season', Numeric(16, 4)),
					Column('Income_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Qtr', Numeric(16, 4)),
					Column('GP_Margin_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('IPS_Qtr', Numeric(32, 21)),
					Column('EPS_Qtr', Numeric(32, 21)),
					Column('ROE_Qtr', Numeric(16, 4)),
					Column('Income_Growth_Pre', Numeric(16, 4)),
					Column('NPPC_Growth_Pre', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre', Numeric(32, 13)),
					Column('GP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('Income_Growth_YOY', Numeric(16, 4)),
					Column('NPPC_Growth_YOY', Numeric(16, 4)),
					Column('NPLNRP_Growth_YOY', Numeric(32, 13)),
					Column('GP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('IPS', Numeric(32, 21)),
					Column('EPS', Numeric(32, 21)),
					Column('CFPS', Numeric(32, 21)),
					Column('Pre_IPS', Numeric(32, 21)),
					Column('Pre_EPS', Numeric(32, 21)),
					Column('Pre_CFPS', Numeric(32, 21)),
					Column('YOY_IPS', Numeric(32, 21)),
					Column('YOY_EPS', Numeric(32, 21)),
					Column('YOY_CFPS', Numeric(32, 21)),
					Column('rPE', Numeric(32, 10)),
					Column('rPB', Numeric(32, 10)),
					Column('rPS', Numeric(32, 10)),
					Column('rPCF', Numeric(32, 10)),
					Column('SettlePrice', Numeric(16, 10)),
					Column('IndusInnerCode', Integer()),
					Column('IndusCreatePrice', Numeric(16, 10)),
					Column('IndusSettlePrice', Numeric(16, 10)),
					Column('ExecutivesProp', Numeric(16, 10)),
					Column('InstitutionNum', Integer()),
					Column('InstitutionProp', Numeric(16, 10)),
					Column('RegionScore', Integer()),
					Column('ExeClosePrice_CreateDate_Wind', Numeric(16, 4)),
					Column('ExeClosePrice_SettleDate_Wind', Numeric(16, 4)),
					Column('Momentum20Day', Numeric(20, 4)),
					Column('Momentum40Day', Numeric(20, 4)),
					Column('Momentum60Day', Numeric(20, 4)),
					Column('Momentum120Day', Numeric(20, 4)),
					Column('Momentum180Day', Numeric(20, 4)),
					Column('Momentum240Day', Numeric(20, 4)),
					Column('PriceDiff', Numeric(20, 4)),
					Column('DayDiff', Integer()),
					Column('TurnoverRatio', Numeric(20, 4)),
					Column('AvgTurnoverPrice', Numeric(20, 4)),
					Column('AvgTurnoverPriceFactor', Numeric(20, 4)),
					Column('AvgTurnoverRatio5Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio10Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio20Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio40Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio60Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio120Day', Numeric(20, 5)),
					)

	insert(conn, residtb, df)

def insertMonthly(conn, df):
	"""	插入每月因子残差数据表
	"""
	metadata = MetaData()
	residtb = Table('FactoMonthlyResidual', metadata,
					Column('TradingDay', DateTime(), nullable=False),
					Column('SecuCode', String(10), nullable=False),
					Column('ClosePrice', Numeric(16, 4)),
					Column('OpenPrice', Numeric(16, 4)),
					Column('HighPrice', Numeric(16, 4)),
					Column('LowPrice', Numeric(16, 4)),
					Column('ExeClosePrice', Numeric(16, 4)),
					Column('ExeOpenPrice', Numeric(16, 4)),
					Column('ExeHighPrice', Numeric(16, 4)),
					Column('ExeLowPrice', Numeric(16, 4)),
					Column('NonRestrictedShares', Numeric(16, 0)),
					Column('AFloats', Numeric(16, 0)),
					Column('TotalShares', Numeric(16, 0)),
					Column('TurnoverVolume', Numeric(20, 0)),
					Column('NonRestrictedCap', Numeric(36, 4)),
					Column('AFloatsCap', Numeric(36, 4)),
					Column('TotalCap', Numeric(36, 4)),
					Column('SecuAbbr', String(50)),
					Column('IndustrySecuCode_I', String(10)),
					Column('PE', Numeric(38, 6)),
					Column('PB', Numeric(38, 6)),
					Column('PS', Numeric(38, 6)),
					Column('PCF', Numeric(38, 6)),
					Column('DividendYield', Numeric(16, 6)),
					Column('DividendRatio', Numeric(16, 6)),
					Column('TTMIncome', Numeric(36, 4)),
					Column('GP_Margin', Numeric(16, 4)),
					Column('NP_Margin', Numeric(16, 4)),
					Column('ROA', Numeric(16, 4)),
					Column('ROE', Numeric(16, 4)),
					Column('AssetsTurnover', Numeric(16, 4)),
					Column('EquityTurnover', Numeric(16, 4)),
					Column('Cash_to_Assets', Numeric(16, 4)),
					Column('Liability_to_Assets', Numeric(16, 4)),
					Column('EquityMultiplier', Numeric(16, 4)),
					Column('CurrentRatio', Numeric(16, 4)),
					Column('Income_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('Income_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Season', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Season', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre_Season', Numeric(32, 14)),
					Column('NPLNRP_Growth_YOY_Season', Numeric(32, 15)),
					Column('Income_Growth_Pre_Season', Numeric(16, 4)),
					Column('Income_Growth_YOY_Season', Numeric(16, 4)),
					Column('Income_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Qtr', Numeric(16, 4)),
					Column('GP_Margin_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('IPS_Qtr', Numeric(32, 21)),
					Column('EPS_Qtr', Numeric(32, 21)),
					Column('ROE_Qtr', Numeric(16, 4)),
					Column('Income_Growth_Pre', Numeric(16, 4)),
					Column('NPPC_Growth_Pre', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre', Numeric(32, 13)),
					Column('GP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('Income_Growth_YOY', Numeric(16, 4)),
					Column('NPPC_Growth_YOY', Numeric(16, 4)),
					Column('NPLNRP_Growth_YOY', Numeric(32, 13)),
					Column('GP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('IPS', Numeric(32, 21)),
					Column('EPS', Numeric(32, 21)),
					Column('CFPS', Numeric(32, 21)),
					Column('Pre_IPS', Numeric(32, 21)),
					Column('Pre_EPS', Numeric(32, 21)),
					Column('Pre_CFPS', Numeric(32, 21)),
					Column('YOY_IPS', Numeric(32, 21)),
					Column('YOY_EPS', Numeric(32, 21)),
					Column('YOY_CFPS', Numeric(32, 21)),
					Column('rPE', Numeric(32, 10)),
					Column('rPB', Numeric(32, 10)),
					Column('rPS', Numeric(32, 10)),
					Column('rPCF', Numeric(32, 10)),
					Column('SettlePrice', Numeric(16, 10)),
					Column('IndusInnerCode', Integer()),
					Column('IndusCreatePrice', Numeric(16, 10)),
					Column('IndusSettlePrice', Numeric(16, 10)),
					Column('ExecutivesProp', Numeric(16, 10)),
					Column('InstitutionNum', Integer()),
					Column('InstitutionProp', Numeric(16, 10)),
					Column('RegionScore', Integer()),
					Column('ExeClosePrice_CreateDate_Wind', Numeric(16, 4)),
					Column('ExeClosePrice_SettleDate_Wind', Numeric(16, 4)),
					Column('Momentum20Day', Numeric(20, 4)),
					Column('Momentum40Day', Numeric(20, 4)),
					Column('Momentum60Day', Numeric(20, 4)),
					Column('Momentum120Day', Numeric(20, 4)),
					Column('Momentum180Day', Numeric(20, 4)),
					Column('Momentum240Day', Numeric(20, 4)),
					Column('PriceDiff', Numeric(20, 4)),
					Column('DayDiff', Integer()),
					Column('TurnoverRatio', Numeric(20, 4)),
					Column('AvgTurnoverPrice', Numeric(20, 4)),
					Column('AvgTurnoverPriceFactor', Numeric(20, 4)),
					Column('AvgTurnoverRatio5Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio10Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio20Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio40Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio60Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio120Day', Numeric(20, 5)),
					)

	insert(conn, residtb, df)

def insertResid(df):
	"""	向数据库表advancedb.dbo.FactorDailyResidual中插入数据
		df - pandas DataFrame对象
	"""
	engine = dbaccessor.getdb('localhost', 'zhenggq', 'Yuzhong0931', 'advancedb', 1433)
	conn = engine.connect()
	metadata = MetaData()
	
	residtb = Table('FactorDailyResidual', metadata,
					Column('TradingDay', DateTime(), nullable=False),
					Column('SecuCode', String(10), nullable=False),
					Column('ClosePrice', Numeric(16, 4)),
					Column('OpenPrice', Numeric(16, 4)),
					Column('HighPrice', Numeric(16, 4)),
					Column('LowPrice', Numeric(16, 4)),
					Column('ExeClosePrice', Numeric(16, 4)),
					Column('ExeOpenPrice', Numeric(16, 4)),
					Column('ExeHighPrice', Numeric(16, 4)),
					Column('ExeLowPrice', Numeric(16, 4)),
					Column('NonRestrictedShares', Numeric(16, 0)),
					Column('AFloats', Numeric(16, 0)),
					Column('TotalShares', Numeric(16, 0)),
					Column('TurnoverVolume', Numeric(20, 0)),
					Column('NonRestrictedCap', Numeric(36, 4)),
					Column('AFloatsCap', Numeric(36, 4)),
					Column('TotalCap', Numeric(36, 4)),
					Column('SecuAbbr', String(50)),
					Column('IndustrySecuCode_I', String(10)),
					Column('PE', Numeric(38, 6)),
					Column('PB', Numeric(38, 6)),
					Column('PS', Numeric(38, 6)),
					Column('PCF', Numeric(38, 6)),
					Column('DividendYield', Numeric(16, 6)),
					Column('DividendRatio', Numeric(16, 6)),
					Column('TTMIncome', Numeric(36, 4)),
					Column('GP_Margin', Numeric(16, 4)),
					Column('NP_Margin', Numeric(16, 4)),
					Column('ROA', Numeric(16, 4)),
					Column('ROE', Numeric(16, 4)),
					Column('AssetsTurnover', Numeric(16, 4)),
					Column('EquityTurnover', Numeric(16, 4)),
					Column('Cash_to_Assets', Numeric(16, 4)),
					Column('Liability_to_Assets', Numeric(16, 4)),
					Column('EquityMultiplier', Numeric(16, 4)),
					Column('CurrentRatio', Numeric(16, 4)),
					Column('Income_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('Income_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Season', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Season', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre_Season', Numeric(32, 14)),
					Column('NPLNRP_Growth_YOY_Season', Numeric(32, 15)),
					Column('Income_Growth_Pre_Season', Numeric(16, 4)),
					Column('Income_Growth_YOY_Season', Numeric(16, 4)),
					Column('Income_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Qtr', Numeric(16, 4)),
					Column('GP_Margin_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('IPS_Qtr', Numeric(32, 21)),
					Column('EPS_Qtr', Numeric(32, 21)),
					Column('ROE_Qtr', Numeric(16, 4)),
					Column('Income_Growth_Pre', Numeric(16, 4)),
					Column('NPPC_Growth_Pre', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre', Numeric(32, 13)),
					Column('GP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('Income_Growth_YOY', Numeric(16, 4)),
					Column('NPPC_Growth_YOY', Numeric(16, 4)),
					Column('NPLNRP_Growth_YOY', Numeric(32, 13)),
					Column('GP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('IPS', Numeric(32, 21)),
					Column('EPS', Numeric(32, 21)),
					Column('CFPS', Numeric(32, 21)),
					Column('Pre_IPS', Numeric(32, 21)),
					Column('Pre_EPS', Numeric(32, 21)),
					Column('Pre_CFPS', Numeric(32, 21)),
					Column('YOY_IPS', Numeric(32, 21)),
					Column('YOY_EPS', Numeric(32, 21)),
					Column('YOY_CFPS', Numeric(32, 21)),
					Column('rPE', Numeric(32, 10)),
					Column('rPB', Numeric(32, 10)),
					Column('rPS', Numeric(32, 10)),
					Column('rPCF', Numeric(32, 10)),
					Column('SettlePrice', Numeric(16, 10)),
					Column('IndusInnerCode', Integer()),
					Column('IndusCreatePrice', Numeric(16, 10)),
					Column('IndusSettlePrice', Numeric(16, 10)),
					Column('ExecutivesProp', Numeric(16, 10)),
					Column('InstitutionNum', Integer()),
					Column('InstitutionProp', Numeric(16, 10)),
					Column('RegionScore', Integer()),
					Column('ExeClosePrice_CreateDate_Wind', Numeric(16, 4)),
					Column('ExeClosePrice_SettleDate_Wind', Numeric(16, 4)),
					Column('Momentum20Day', Numeric(20, 4)),
					Column('Momentum40Day', Numeric(20, 4)),
					Column('Momentum60Day', Numeric(20, 4)),
					Column('Momentum120Day', Numeric(20, 4)),
					Column('Momentum180Day', Numeric(20, 4)),
					Column('Momentum240Day', Numeric(20, 4)),
					Column('PriceDiff', Numeric(20, 4)),
					Column('DayDiff', Integer()),
					Column('TurnoverRatio', Numeric(20, 4)),
					Column('AvgTurnoverPrice', Numeric(20, 4)),
					Column('AvgTurnoverPriceFactor', Numeric(20, 4)),
					Column('AvgTurnoverRatio5Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio10Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio20Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio40Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio60Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio120Day', Numeric(20, 5)),
					)
	
	ins = residtb.insert()
	dataList = df.T.to_dict().values()

	try:
		result = conn.execute(ins, dataList)
	finally:
		conn.close()
	
