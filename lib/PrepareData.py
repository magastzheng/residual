#coding=utf-8
import xlrd
import re
import pandas as pd
import numpy as np
import pdb
from dateutil.parser import parse
import os
import matplotlib.pyplot as plt
import pymssql
import sqlalchemy
import datetime
import sys

import random
random.seed(3)
import logging
import Utils
reload(Utils)

##########################
# FactorMonthly_Comparable
##########################
# ²ÎÊý
boolForceToUpdate = True
boolDiscretize = False
NYearTrain = 3

#dtBackTestStart = Utils.dtBackTestStart
dtBackTestStart = datetime.datetime(2005, 1, 1)
listDTMonth = pd.date_range(
        datetime.datetime(dtBackTestStart.year - NYearTrain, dtBackTestStart.month, dtBackTestStart.day),
        datetime.datetime.now(), freq='1M')
listDTMonth = [datetime.datetime(2017, 1, 31)]

for dtMonth in listDTMonth:
    # read from SQL
    sql = '''
    SELECT
    *
    FROM 
    baozh.dbo.FactorMonthly_Comparable
    WHERE
    createDate = '{0}'
    and SecuAbbr not like '%ST%'
    order by 
    createDate
    '''.format(
            dtMonth.strftime('%Y%m%d') 
            )
    engine = Utils.connect()
    df = pd.read_sql(sql, engine)
    df['createDate'] = df['createDate'].apply(lambda x: parse(str(int(x))))
    df['settleDate'] = df['settleDate'].apply(lambda x: parse(str(int(x))))

    # remove 10000
    df = df.replace(10000, np.nan)
    df['TurnoverVolume'] = df['TurnoverVolume'].replace(0, np.nan)
    df['TurnoverRatio'] = df['TurnoverRatio'].replace(0, np.nan)

    # standardize inside industry
    df = df.reset_index().groupby('IndustrySecuCode_I').apply(Utils.funcStandardize)

    # TODO: add 29 columns of 01, indicating industry

    # TODO: OLS
    #for strFactor in Utils.listFactorAll:
    for strFactor in ['Momentum20Day']:
        import statsmodels.formula.api as sm
        dfXY = df[['NonRestrictedCap', strFactor]].dropna()
        #ols = sm.ols(formula='Momentum20Day~NonRestrictedCap', data=dfXY).fit()
        ols = sm.OLS(dfXY['NonRestrictedCap'], dfXY[strFactor]).fit()
        strFactorResidual = strFactor + 'Residual'
        df[strFactorResidual] = np.nan
        df.ix[dfXY.index, strFactorResidual] = ols.resid
        print ols.resid
        print ols.summary()
    
    # discretize 
    if boolDiscretize:
        df = Utils.funcDiscretize(df)
