#coding: utf8

from sqlalchemy.sql import select, update, insert
from sqlalchemy.orm import Session
from sqlalchemy import (
				MetaData, 
				Table, 
				Column,
				Integer,
				Float,
				Numeric,
				String,
				DateTime,
				ForeignKey
				)
import pandas as pd
import numpy as np

def insert(conn, tb, df):
	"""	往指定表中插入数据
		conn - 数据库连接对象
		tb - 数据库表，sqlalchemy.Table类型
		df - pandas DataFrame对象，存放需要插入的所有数据
	"""
	#TODO: verify the result
	ins = tb.insert()
	#df = df.replace(np.nan, None)
	df = df.where((pd.notnull(df)), None)
	dataList = df.T.to_dict().values()
	try:
		#print ins
		result = conn.execute(ins, dataList)
	finally:
		conn.close()


def insertResid(conn, df, tbname):
	"""	往指定表中插入残差数据
		conn - 数据库连接对象
		df - pandas DataFrame对象，存放需要插入的所有数据
		tbname - 数据库表名

		需要注意的是插入的表要有相同的结构，也就是相同列的定义
	"""
	metadata = MetaData()
	residtb = Table(tbname, metadata,
					Column('createDate', Float()),
					Column('settleDate', Float()),
					Column('TradingDay', DateTime(), nullable=False),
					Column('SecuCode', String(10), nullable=False),
					Column('ClosePrice', Float()),
					Column('OpenPrice', Float()),
					Column('HighPrice', Float()),
					Column('LowPrice', Float()),
					Column('ExeClosePrice', Float()),
					Column('ExeOpenPrice', Float()),
					Column('ExeHighPrice', Float()),
					Column('ExeLowPrice', Float()),
					Column('NonRestrictedShares', Float()),
					Column('AFloats', Float()),
					Column('TotalShares', Float()),
					Column('TurnoverVolume', Float()),
					Column('NonRestrictedCap', Float()),
					Column('AFloatsCap', Float()),
					Column('TotalCap', Float()),
					Column('SecuAbbr', String(50)),
					Column('IndustrySecuCode_I', String(10)),
					Column('PE', Float()),
					Column('PB', Float()),
					Column('PS', Float()),
					Column('PCF', Float()),
					Column('DividendYield', Float()),
					Column('DividendRatio', Float()),
					Column('TTMIncome', Float()),
					Column('GP_Margin', Float()),
					Column('NP_Margin', Float()),
					Column('ROA', Float()),
					Column('ROE', Float()),
					Column('AssetsTurnover', Float()),
					Column('EquityTurnover', Float()),
					Column('Cash_to_Assets', Float()),
					Column('Liability_to_Assets', Float()),
					Column('EquityMultiplier', Float()),
					Column('CurrentRatio', Float()),
					Column('Income_Growth_YOY_Comparable', Float()),
					Column('NPPC_Growth_YOY_Comparable', Float()),
					Column('GP_Margin_Comparable', Float()),
					Column('GP_Margin_Growth_YOY_Comparable', Float()),
					Column('NP_Margin_Comparable', Float()),
					Column('NP_Margin_Growth_YOY_Comparable', Float()),
					Column('Income_Growth_Pre_Comparable', Float()),
					Column('NPPC_Growth_Pre_Comparable', Float()),
					Column('GP_Margin_Growth_Pre_Comparable', Float()),
					Column('NP_Margin_Growth_Pre_Comparable', Float()),
					Column('NPPC_Growth_Pre_Season', Float()),
					Column('NPPC_Growth_YOY_Season', Float()),
					Column('NPLNRP_Growth_Pre_Season', Float()),
					Column('NPLNRP_Growth_YOY_Season', Float()),
					Column('Income_Growth_Pre_Season', Float()),
					Column('Income_Growth_YOY_Season', Float()),
					Column('Income_Growth_Qtr_Comparable', Float()),
					Column('NPPC_Growth_Qtr_Comparable', Float()),
					Column('GP_Margin_Qtr', Float()),
					Column('GP_Margin_Growth_Qtr_Comparable', Float()),
					Column('IPS_Qtr', Float()),
					Column('EPS_Qtr', Float()),
					Column('ROE_Qtr', Float()),
					Column('Income_Growth_Pre', Float()),
					Column('NPPC_Growth_Pre', Float()),
					Column('NPLNRP_Growth_Pre', Float()),
					Column('GP_Margin_Growth_Pre', Float()),
					Column('NP_Margin_Growth_Pre', Float()),
					Column('Income_Growth_YOY', Float()),
					Column('NPPC_Growth_YOY', Float()),
					Column('NPLNRP_Growth_YOY', Float()),
					Column('GP_Margin_Growth_YOY', Float()),
					Column('NP_Margin_Growth_YOY', Float()),
					Column('IPS', Float()),
					Column('EPS', Float()),
					Column('CFPS', Float()),
					Column('Pre_IPS', Float()),
					Column('Pre_EPS', Float()),
					Column('Pre_CFPS', Float()),
					Column('YOY_IPS', Float()),
					Column('YOY_EPS', Float()),
					Column('YOY_CFPS', Float()),
					Column('rPE', Float()),
					Column('rPB', Float()),
					Column('rPS', Float()),
					Column('rPCF', Float()),
					Column('SettlePrice', Float()),
					Column('IndusInnerCode', Integer()),
					Column('IndusCreatePrice', Float()),
					Column('IndusSettlePrice', Float()),
					Column('ExecutivesProp', Float()),
					Column('InstitutionNum', Float()),
					Column('InstitutionProp', Float()),
					Column('RegionScore', Float()),
					Column('ExeClosePrice_CreateDate_Wind', Float()),
					Column('ExeClosePrice_SettleDate_Wind', Float()),
					Column('Momentum20Day', Float()),
					Column('Momentum40Day', Float()),
					Column('Momentum60Day', Float()),
					Column('Momentum120Day', Float()),
					Column('Momentum180Day', Float()),
					Column('Momentum240Day', Float()),
					Column('PriceDiff', Float()),
					Column('DayDiff', Float()),
					Column('TurnoverRatio', Float()),
					Column('AvgTurnoverPrice', Float()),
					Column('AvgTurnoverPriceFactor', Float()),
					Column('AvgTurnoverRatio5Day', Float()),
					Column('AvgTurnoverRatio10Day', Float()),
					Column('AvgTurnoverRatio20Day', Float()),
					Column('AvgTurnoverRatio40Day', Float()),
					Column('AvgTurnoverRatio60Day', Float()),
					Column('AvgTurnoverRatio120Day', Float()),
                    Column('E_EPS', Float()),
					Column('E_growth', Float()),
					Column('FreeFloat_Total', Float()),
					Column('EPS_CFPS', Float()),
					)

	insert(conn, residtb, df)
