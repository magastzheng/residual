#coding=utf-8

import dbaccessor
import pandas
import datetime
import fileutil
import dataapi

def getSqlFormat(filename):
	return fileutil.readFile(filename)

def getMonthlyStockDataSql(secucode, path):
	filename = 'sql/stockdeltaregress.sql'
	filepath = '{0}{1}'.format(path, filename)
	fmt = getSqlFormat(filepath)

	return fmt.format(secucode)

def getMonthlyStockData(secucode, path):
	sql = getMonthlyStockDataSql(secucode, path)

	engine = dataapi.getengine()
	df = dbaccessor.query(engine, sql)
	return df

def getFactorMonthlyDataSql(td, path):
	filename = 'sql/queryfactormonthly.sql'
	filepath = '{0}{1}'.format(path, filename)
	fmt = getSqlFormat(filepath)

	return fmt.format(td.strftime('%Y%m%d'))

def getFactorMonthlyData(td, path):
	sql = getFactorMonthlyDataSql(td, path)
	
	engine = dataapi.getengine()
	df = dbaccessor.query(engine, sql)
	return df
