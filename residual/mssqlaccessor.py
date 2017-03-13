import pymssql

def getConnection(host, user, password, dbname):
		return pymssql.connect(host, user, password, dbname)

def getCursor(conn):
		return conn.cursor()

