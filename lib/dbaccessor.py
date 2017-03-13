#encoding=utf-8

import sqlalchemy
import pymssql
import pandas as pd

def getdb(host, userName, passWord):
		"""Open the database connection
		"""
		constr = 'mssql+pymssql://%s:%s@%s'%(userName, passWord, host)
		print 'Open database: '+constr

		engine = sqlalchemy.create_engine(constr)
		return engine

def query(engine, sql):
		"""Execute a sql query. It will return an pandas.DataFrame object and then close the connection"""
		conn = engine.connect()
		#execute sql here
		resultProxy = conn.execute(sql) 

		#convert the sa data to pandas DataFrame
		df = pd.DataFrame(resultProxy.fetchall(), columns=resultProxy.keys())
		conn.close()

		return df

def raw_query(engine, sql):
		"""Execute a raw_query. It will return a ResultProxy object. Caller needs to close it after finishing."""
		conn = engine.connect()
		#execute sql here
		resultProxy = conn.execute(sql) 

		return resultProxy

if __name__ == '__main__':
		sql = 'select * from zhenggq.dbo.securitybasicinfo'
		engine = getdb('176.1.11.55', 'zhenggq', 'Yuzhong0931')
		#result = query(engine, sql)
		result = raw_query(engine, sql)
		#print result
		print result.keys()
		for k in result.keys():
			print k
