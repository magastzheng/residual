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

def getFactorDataSql(td, filename, path):
	filepath = '{0}/sql/{1}'.format(path, filename)
	fmt = getSqlFormat(filepath)

	return fmt.format(td.strftime('%Y%m%d'))

def getFactorMonthlyDataSql(td, path):
	filename = 'sql/queryfactormonthly.sql'
	filepath = '{0}{1}'.format(path, filename)
	fmt = getSqlFormat(filepath)

	return fmt.format(td.strftime('%Y%m%d'))

def getFactorMonthlyData(td, path):
	sql = getFactorDataSql(td, 'queryfactormonthly.sql', path)

	engine = dataapi.getengine()
	df = dbaccessor.query(engine, sql)
	return df

fdtypes = {
	'd': None,
	'w': None,
	'm': getFactorMonthlyData
}

def getFactorData(dtype, td, path):
	fn = fdtypes.get(dtype, getFactorMonthlyData)
	df = None
	if fn is not None:
		df = fn(td, path)
	
	return df
