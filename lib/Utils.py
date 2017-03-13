#coding=gbk
import xlrd
import re
import pandas
import numpy
from dateutil.parser import parse
import os
import matplotlib.pyplot as plt
import pymssql
import sqlalchemy
import datetime
import sys
import pdb

import random

import logging
import gc
import scipy

boolTest = True # 如果只是为了测试程序

dirRawData = './RawData/'
dirTradingDataDaily = dirRawData + 'TradingDataDaily/'
dirFactorMonthlyProcessed = dirRawData + 'FactorMonthlyProcessed/'
dirFactorMonthlyRaw = dirRawData + 'FactorMonthlyRaw/'
dirIndexData = dirRawData + 'IndexData/'
dirSecurityBasicInfo = dirRawData + 'SecurityBasicInfo/'


listFactorAll = ['Income_Growth_Pre', 'NPPC_Growth_Pre', 'Income_Growth_YOY', 'NPPC_Growth_YOY', 'ROA', 'ROE', 'GP_Margin', 'NP_Margin', 'GP_Margin_Growth_Pre', 'NP_Margin_Growth_Pre', 'GP_Margin_Growth_YOY', 'NP_Margin_Growth_YOY', 'EPS', 'IPS', 'PE', 'rPE', 'PB', 'rPB', 'PS', 'rPS', 'PCF', 'rPCF', 'CFPS', 'DividendYield', 'DividendRatio', 'AssetsTurnover', 'EquityTurnover', 'Liability_to_Assets', 'NonRestrictedCap', 'TotalCap', 'TurnoverVolume', 'TurnoverRatio', 'DayDiff', 'PriceDiff', 'Momentum20Day', 'Momentum40Day', 'Momentum60Day', 'Momentum120Day', 'Momentum180Day', 'Momentum240Day']
listFactorTrading = ['EPS', 'IPS', 'PE', 'rPE', 'PB', 'rPB', 'PS', 'rPS', 'PCF', 'rPCF', 'CFPS', 'TurnoverVolume', 'TurnoverRatio', 'DayDiff', 'PriceDiff', 'Momentum20Day', 'Momentum40Day', 'Momentum60Day', 'Momentum120Day', 'Momentum180Day', 'Momentum240Day']

##########################################################
# 给股票打分
##########################################################

def connect(): 
    # connect to database
    hostName = "176.1.11.55"
    dbName = "baozh"
    tbName = "FactorTradingDay_Comparable"
    userName = 'zhenggq'
    password = 'Yuzhong0931'
    engine = sqlalchemy.create_engine('mssql+pymssql://%s:%s@176.1.11.55'%(userName, password))
    return engine

# get index (proxy for the market performance)
# 万得全A: 881001, HS300EW 000982, HS300 000300, ZZ800 000906, ZZ800EW 000842
def getIndex(SecuCode='000982', boolResampleMonthly=True):
    strFileAddress = dirIndexData + SecuCode + '.pickle'
    if os.path.exists(strFileAddress):
        dfWDQA = pandas.read_pickle(strFileAddress)
    else:
        sql = '''
        SELECT 
        TradingDay,
        ExeClosePrice
        FROM zhenggq.dbo.indextradingdata
        WHERE SecuCode = '{0}'
        ORDER BY TradingDay
        '''.format(
                SecuCode
                )
        engine = connect()
        dfWDQA = pandas.read_sql(sql, engine)
        if os.path.exists(dirIndexData) is False:
            os.mkdir(dirIndexData)
        dfWDQA.to_pickle(strFileAddress)
    dfWDQA['TradingDay'] = dfWDQA['TradingDay'].apply(lambda x: parse(str(int(x))))
    dfWDQA = dfWDQA.set_index('TradingDay')
    if boolResampleMonthly:
        dfWDQA = dfWDQA.resample('1D').ffill().resample('1M').last()
    dfWDQA['ReturnOfMarket'] = dfWDQA['ExeClosePrice'].pct_change()
    dfWDQA = dfWDQA.dropna()
    return dfWDQA

# 获得上市日期，在dtEnd, 不考虑上市不满半年的股票
def getSecurityBasicInfo(dtEnd):
    strFileAddress = dirSecurityBasicInfo + dtEnd.strftime('%Y%m%d') + '.pickle'
    if os.path.exists(strFileAddress):
        df = pandas.read_pickle(strFileAddress)
    else:
        dtLatestIPO = dtEnd - datetime.timedelta(365 / 2) # 不考虑上市不满半年的股票
        sql = '''
        SELECT 
        *
        FROM zhenggq.dbo.securitybasicinfo
        WHERE 
        IPODay < '{0}'
        '''.format(
                dtLatestIPO.strftime('%Y%m%d')
                )
        engine = connect()
        df = pandas.read_sql(sql, engine)
        if os.path.exists(dirSecurityBasicInfo) is False:
            os.mkdir(dirSecurityBasicInfo)
        df.to_pickle(strFileAddress)
    df['IPODay'] = df['IPODay'].apply(lambda x: parse(str(int(x))))
    #df['DelistingDay'] = df['DelistingDay'].apply(lambda x: parse(str(int(x))))
    return df

# get processed data monthly
def getFactorMonthly(dtMonth):
    strFileAddress = dirFactorMonthlyProcessed + dtMonth.strftime('%Y%m%d') + '.pickle'
    df = pandas.read_pickle(strFileAddress)
    return df

# 获得训练窗口中的所有数据
def getDataChunk(dtEnd, SecuCodeIndex, strMethodDiscretization='Histogram10', strMethodDiscretizeY='two', NYearTrain=5, strTrainRandom='MCFuture0'):
    # 读取训练用的数据
    strFileAddress = './RawData/df20040131_20160731.pickle' # PrepareDataForMC.py计算
    df = pandas.read_pickle(strFileAddress)

    if strTrainRandom.startswith('MCFuture'):
        SEED = re.compile(r'MCFuture(\d+)').match(strTrainRandom).groups()[0]
        SEED = int(SEED)
        random.seed(SEED)
        dtBackTestStart = datetime.datetime(2007, 1, 1)
        listDTMonth = pandas.date_range(
                datetime.datetime(dtBackTestStart.year - NYearTrain, dtBackTestStart.month, 1),
                datetime.datetime.now(), freq='1M')
        listDTMonth = listDTMonth[(listDTMonth < dtEnd) | (listDTMonth > dtEnd + datetime.timedelta(365*2))]
        listDTMonth = random.sample(listDTMonth, NYearTrain*12)
        listDTMonth.append(dtEnd)
        df = df[df['createDate'].isin(listDTMonth)]
    elif strTrainRandom.startswith('MC'):
        SEED = re.compile(r'MC(\d+)').match(strTrainRandom).groups()[0]
        SEED = int(SEED)
        random.seed(SEED)
        dtBackTestStart = datetime.datetime(2007, 1, 1)
        listDTMonth = pandas.date_range(
                datetime.datetime(dtBackTestStart.year - NYearTrain, dtBackTestStart.month, 1),
                datetime.datetime.now(), freq='1M')
        listDTMonth = listDTMonth[listDTMonth < dtEnd]
        listDTMonth = random.sample(listDTMonth, NYearTrain*12)
        listDTMonth.append(dtEnd)
        df = df[df['createDate'].isin(listDTMonth)]
    else:
        listDTMonth = pandas.date_range(
                datetime.datetime(dtEnd.year - NYearTrain, dtEnd.month, 1),
                dtEnd, freq='1M')
        df = df[df['createDate'].isin(listDTMonth)]

    # 对每个因子的时间序列，做标准化
    #df = funcStandardize(df)

    # 计算出用于训练的 Y
    df['StateObserved'] = 99
    strSignalForState = 'ReturnRelative'
    if strMethodDiscretizeY == 'two':
        df.ix[df[strSignalForState] >= 0, 'StateObserved'] = 1
        df.ix[df[strSignalForState] < 0, 'StateObserved'] = 0
    elif strMethodDiscretizeY == 'three':
        df.ix[df[strSignalForState] >= df.ix[df.index!=dtEnd, strSignalForState].quantile(0.7), 'StateObserved'] = 1
        df.ix[df[strSignalForState] < df[df.index!=dtEnd, strSignalForState].quantile(0.3), 'StateObserved'] = 0
    df['StateObserved'] = df['StateObserved'].astype(numpy.int32)  # 为了 hmmlearn Cython

    # 离散化之前 保留float信息
    df['NonRestrictedCap_float'] = df['NonRestrictedCap']
    df['Momentum20Day_float'] = df['Momentum20Day']
    df['Momentum40Day_float'] = df['Momentum40Day']
    df['Momentum60Day_float'] = df['Momentum60Day']
    df['Momentum120Day_float'] = df['Momentum120Day']

    # 离散化 (如果在PrepareData里没做的话)
    if strMethodDiscretization != -1:
        pattern = re.compile(r'(\D+)(\d+)')
        listMatched = pattern.match(strMethodDiscretization).groups()
        strFuncDiscretize = listMatched[0]
        NBinDiscretize = int(listMatched[1])
        if strFuncDiscretize == 'Histogram':
            gg = df.groupby('createDate')
            df = gg.apply(funcDiscretize, NBinDiscretize)
        elif strFuncDiscretize == 'Cluster':
            df = funcDiscretizeCluster(df, NBinDiscretize)
        elif strFuncDiscretize == 'Entropy':
            df = funcDiscretizeEntropy(df, NBinDiscretize)
        elif strFuncDiscretize == 'Value':
            df = funcDiscretizeValue(df, NBinDiscretize)
        else:
            gg = df.groupby('createDate')
            df = gg.apply(funcDiscretize, NBinDiscretize)
            strToLog = 'unknown discretization method %s'%strFuncDiscretize
            logging.log(logging.ERROR, strToLog)
    
        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)
        df = df.reset_index()
        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)
    return df
    
# generate models with different observations
def generateFactorList(SEED_MODEL_SELECT, dictNumFactor):
    random.seed(SEED_MODEL_SELECT)
    listListFactorHMM = []
    strMethodCombination = dictNumFactor['strMethodCombination']
    NModel = dictNumFactor['NModel']
    listFactorTrading = dictNumFactor['listFactorTrading']
    listFactorAll = dictNumFactor['listFactorAll']
    if strMethodCombination == 'random':
        listFactorFundamental = list(set(listFactorAll).difference(set(listFactorTrading)))
        NFactorTrading = dictNumFactor['NFactorTrading']
        NFactorFundamental = dictNumFactor['NFactorFundamental']
        NFactorAll = dictNumFactor['NFactorAll']
        for nModel in range(0, NModel):
            listFactorHMM = []
            n = 0
            while n < NFactorTrading:
                strFactor = listFactorTrading[random.randint(0, len(listFactorTrading)-1)]
                if strFactor not in listFactorHMM:
                    listFactorHMM += [strFactor]
                    n = n + 1
            n = 0
            while n < NFactorFundamental:
                strFactor = listFactorFundamental[random.randint(0, len(listFactorFundamental)-1)]
                if strFactor not in listFactorHMM:
                    listFactorHMM += [strFactor]
                    n = n + 1
            n = 0
            while n < NFactorAll:
                strFactor = listFactorAll[random.randint(0, len(listFactorAll)-1)]
                if strFactor not in listFactorHMM:
                    listFactorHMM += [strFactor]
                    n = n + 1
            listListFactorHMM.append(listFactorHMM)
    elif strMethodCombination == 'comb':
        NFactorTrading = dictNumFactor['NFactorTrading']
        NFactorFundamental = dictNumFactor['NFactorFundamental']
        NFactorAll = dictNumFactor['NFactorAll']
        listFactorFundamental = list(set(listFactorAll).difference(set(listFactorTrading)))
        import itertools
        listListFactorTrading = list(itertools.combinations(listFactorTrading, NFactorTrading))
        listListFactorFundamental = list(itertools.combinations(listFactorFundamental, NFactorFundamental))
        listListFactorHMM = []
        for listFactorTrading in listListFactorTrading:
            for listFactorFundamental in listListFactorFundamental:
                listFactorHMM = []
                for element in listFactorTrading:
                    listFactorHMM.append(element)
                for element in listFactorFundamental:
                    listFactorHMM.append(element)
                listListFactorHMM.append(listFactorHMM)
        
    return listListFactorHMM

def generateFactorListCustomized(SEED_MODEL_SELECT, dictNumFactor):
    random.seed(SEED_MODEL_SELECT)
    listListFactorHMM = []
    strMethodCombination = dictNumFactor['strMethodCombination']
    if dictNumFactor.has_key('listFactorAll'):
        listFactorAll = dictNumFactor['listFactorAll']

    if strMethodCombination == 'random':
        NModel = dictNumFactor['NModel']
        NFactorAll = dictNumFactor['NFactorAll']
        for nModel in range(0, NModel):
            listFactorHMM = []
            n = 0
            while n < NFactorAll:
                strFactor = listFactorAll[random.randint(0, len(listFactorAll)-1)]
                if strFactor not in listFactorHMM:
                    listFactorHMM += [strFactor]
                    n = n + 1
            listListFactorHMM.append(listFactorHMM)
    elif strMethodCombination == 'comb':
        NSelected = dictNumFactor['NSelected']
        import itertools
        listListFactorHMM = list(itertools.combinations(listFactorAll, NSelected))
    return listListFactorHMM
# 标准化数据
def funcStandardize(df):
    for strFactor in listFactorAll:
        if strFactor in df.columns:
            series = df[strFactor].dropna()
            if series.empty:
                df[strFactor] = 0
                continue
            mu = series.mean()
            std = series.std()
            if std == 0:
                df[strFactor] = 0
            else:
                df[strFactor] = df[strFactor].apply(lambda x: (x - mu) / std)
            upperLimit = df[strFactor].quantile(0.95)
            middle = df[strFactor].quantile(0.5)
            lowerLimit = df[strFactor].quantile(0.05)
            df[strFactor] = df[strFactor].apply(lambda x: min(max(x, lowerLimit), upperLimit))
    return df

# 离散化-Decision Tree
from sklearn import tree
def funcCutX(x, listXCut):
    if numpy.isnan(x):
        return x
    for nCut, xCut in enumerate(listXCut):
        if x <= xCut:
            return nCut + 1
    return len(listXCut) + 1

def findCutXBySKLearn(df, strX, strY, NCut):
    estimator = tree.DecisionTreeClassifier(
            criterion='entropy',
            max_depth=numpy.log2(NCut),
            min_samples_leaf=max(1, df.index.size / NCut / 4)
            )
    dfSub = df[[strX, strY]].dropna()
    X = dfSub[strX].values.reshape((dfSub.index.size, 1))
    Y = dfSub[strY].values.reshape((dfSub.index.size, 1))
    estimator.fit(X, Y)
    threshold = estimator.tree_.threshold
    threshold = threshold[threshold!=-2]
    threshold = numpy.sort(threshold)
    del estimator
    gc.collect()
    return threshold

def funcDiscretizeEntropy(df, NBin=4):
    for strFactor in listFactorAll:
        if strFactor in df.columns:
            listBestCut = findCutXBySKLearn(df[df['createDate']!=df['createDate'].max()], strFactor, 'StateObserved', NBin)
            df[strFactor] = df[strFactor].apply(funcCutX, args=(listBestCut, ))
    return df

def funcDiscretizeValue(df, NBin=4):
    for strFactor in listFactorAll:
        if strFactor in df.columns:
            xMin = df[strFactor].min()
            xMax = df[strFactor].max()
            xStep = (xMax-xMin) / (NBin-1)
            df[strFactor] = numpy.floor((df[strFactor] - xMin) / xStep)
    return df

# 计算离散化之后的 信息熵，用于判断离散化的有效程度
def calculateEntropy(df, strX, strY):
    dfP = df.groupby(strX)[strY].value_counts() / df.groupby(strX)[strY].count()
    seriesWeight = df.groupby(strX)[strY].count() / df.index.size
    entropy = 0
    for group in dfP.index.get_level_values(strX).unique():
        entropy += scipy.stats.entropy(dfP.ix[group]) * seriesWeight.ix[group]
    entropyEntropyCut = entropy

# 离散化-给所有因子打分
def funcDiscretize(df, NBin=10):
    for strFactor in listFactorAll:
        if strFactor in df.columns:
            NFactor = df[strFactor].size
            df[strFactor] = numpy.ceil(df[strFactor].rank() / float(NFactor) * NBin)
    return df

# 离散化-给所有因子打分, 使用聚类的方法
from sklearn.cluster import KMeans
def funcDiscretizeCluster(df, NBin=10):
    #pdb.set_trace()
    for strFactor in listFactorAll:
        if strFactor in df.columns:
            model = KMeans(n_clusters=NBin)
            series = df[strFactor].dropna()
            indexNotNA = series.index
            array = series.values.reshape((series.size, 1))
            df.ix[indexNotNA, strFactor] = model.fit(array).predict(array)
    return df

# 取TradingDataDaily, 用于给出每日净值
def getTradingDataDaily(dt):
    if os.path.exists(dirTradingDataDaily) is False:
        os.mkdir(dirTradingDataDaily)
    strFileAddress = dirTradingDataDaily + dt.strftime('%Y%m%d') + '.pickle'
    if os.path.exists(strFileAddress):
        df = pandas.read_pickle(strFileAddress)
        return df

    dtNextMonth = dt + datetime.timedelta(1)
    sql = '''
    SELECT
    TradingDay,
    SecuCode,
    ExeClosePrice,
    ExeOpenPrice,
    TurnoverVolume,
    OpenPrice,
    ClosePrice

    FROM baozh.dbo.TradingDataDaily
    WHERE 
    (year(TradingDay) = {0}
    AND month(TradingDay) = {1})
    OR TradingDay = '{2}'
    ORDER BY TradingDay
    '''.format(
            dtNextMonth.year,
            dtNextMonth.month,
            dt.strftime('%Y%m%d')
            )
    engine = connect()
    df = pandas.read_sql(sql, engine)
    df.to_pickle(strFileAddress)
    return df



##########################################################
# 选股，生成Excel报告
##########################################################
# 选择所有模型
def getAllModel(dfResultMonthly):
    listColumnModelAll = []
    for strColumn in dfResultMonthly.columns:
        if strColumn.startswith('Model'):
            listColumnModelAll.append(strColumn)
    return listColumnModelAll

# 把得分从概率转化到01的不同方法 - GiniImpurity
def digitizeByGiniImpurity(seriesX, seriesY, arrayCut):
    ret = seriesX.copy()
    seriesX = seriesX[seriesY != 99]
    seriesY = seriesY[seriesY != 99]
    arrayImpurity = numpy.ones(arrayCut.shape)
    NAll = float(seriesY.index.size)
    for nCut, xCut in enumerate(arrayCut):
        series0 = seriesY[seriesX <= xCut]
        series1 = seriesY[seriesX > xCut]
        seriesP0 = series0.value_counts() / series0.size
        seriesP1 = series1.value_counts() / series1.size
        if seriesP0.size < 2 or seriesP1.size < 2:
            impurity = 1
        else:
            impurity = seriesP0.ix[0] * (1 - seriesP0.ix[0]) + seriesP1.ix[0] * (1 - seriesP1.ix[0])
        
        arrayImpurity[nCut] = impurity

    nCut = arrayImpurity.argmin()
    xCut = arrayCut[nCut]
    ret[ret<=xCut] = 0
    ret[ret>xCut] = 1
    return ret

# 把得分从概率转化到01的不同方法 - Entropy
def digitizeByEntropy(seriesX, seriesY, arrayCut):
    ret = seriesX.copy()
    seriesX = seriesX[seriesY != 99]
    seriesY = seriesY[seriesY != 99]
    arrayImpurity = numpy.ones(arrayCut.shape)
    NAll = float(seriesY.index.size)
    for nCut, xCut in enumerate(arrayCut):
        series0 = seriesY[seriesX <= xCut]
        series1 = seriesY[seriesX > xCut]
        seriesP0 = series0.value_counts() / series0.size
        seriesP1 = series1.value_counts() / series1.size
        if seriesP0.size < 2 or seriesP1.size < 2:
            impurity = 1
        else:
            impurity = -seriesP0.ix[0] * numpy.log2(seriesP0.ix[0]) -seriesP1.ix[0] * numpy.log2(seriesP1.ix[0])
        
        arrayImpurity[nCut] = impurity

    nCut = arrayImpurity.argmin()
    xCut = arrayCut[nCut]
    ret[ret<=xCut] = 0
    ret[ret>xCut] = 1
    return ret

# 把得分从概率转化到01的不同方法 - GiniCoefficient
def digitizeByGiniCoefficient(seriesX, seriesY, arrayCut):
    ret = seriesX.copy()
    seriesX = seriesX[seriesY != 99]
    seriesY = seriesY[seriesY != 99]
    dfXY = pandas.concat([seriesX, seriesY], axis=1)
    dfXY = dfXY.dropna()
    dfXY = dfXY.sort(seriesX.name, ascending=False)

    arrayGini = numpy.ones(arrayCut.shape)
    arrayFP = numpy.ones(arrayCut.shape)
    for nCut, xCut in enumerate(arrayCut):
        rateTP = seriesY[(seriesX > xCut) & (seriesY == 1)].index.size / float(seriesY[seriesY == 1].index.size)
        rateFP = seriesY[(seriesX > xCut) & (seriesY == 0)].index.size / float(seriesY[seriesY == 0].index.size)
        gini = rateTP - rateFP
        arrayGini[nCut] = gini
        arrayFP[nCut] = rateFP
    nCut = arrayGini.argmax()
    xCut = arrayCut[nCut]
    ret[ret<=xCut] = 0
    ret[ret>xCut] = 1
    return ret


# 把得分从概率转化到01
def binaryScore(dfResultMonthly, method='half'):
    arrayCut = numpy.linspace(0.5, 0.6, 21)
    for strColumn in dfResultMonthly.columns:
        if strColumn.startswith('Model'):
            if method == 'half':
                dfResultMonthly[strColumn] = dfResultMonthly[strColumn].apply(numpy.round)
            elif method == 'median':
                median = dfResultMonthly[strColumn].quantile(0.5)
                dfResultMonthly.ix[dfResultMonthly[strColumn] > median, strColumn] = 1
                dfResultMonthly.ix[dfResultMonthly[strColumn] <= median, strColumn] = 0
            elif method == 'GiniImpurity':
                dfResultMonthly[strColumn] = digitizeByGiniImpurity(dfResultMonthly[strColumn], dfResultMonthly['StateObserved'], arrayCut)
            elif method == 'Entropy':
                dfResultMonthly[strColumn] = digitizeByEntropy(dfResultMonthly[strColumn], dfResultMonthly['StateObserved'], arrayCut)
            elif method == 'GiniCoefficient':
                dfResultMonthly[strColumn] = digitizeByGiniCoefficient(dfResultMonthly[strColumn], dfResultMonthly['StateObserved'], arrayCut)
            else:
                dfResultMonthly[strColumn] = dfResultMonthly[strColumn].apply(numpy.round)
    return dfResultMonthly

# 选择模型
def _filterModel_mean(dfResultMonthly, dtSelectStock, percentageModelSelected, dirTotalScore=None):
    indexCreateDate = dfResultMonthly.index.get_level_values('createDate')
    
    strFileAddressHigh = dirTotalScore + '/' + 'ModelMeanReturnHigh' + dtSelectStock.strftime('%Y%m%d') + '.pickle'
    strFileAddressLow = dirTotalScore + '/' + 'ModelMeanReturnLow' + dtSelectStock.strftime('%Y%m%d') + '.pickle'
    
    if os.path.exists(strFileAddressHigh) and os.path.exists(strFileAddressLow):
        seriesModelScoreHigh = pandas.read_pickle(strFileAddressHigh)
        seriesModelScoreLow = pandas.read_pickle(strFileAddressLow)
        seriesModelScore = seriesModelScoreHigh
    else:
        listColumnModelAll = []
        for strColumn in dfResultMonthly.columns:
            if strColumn.startswith('Model'):
                listColumnModelAll.append(strColumn)
        
        dictModelScoreHigh = {}
        dictModelScoreLow = {}
        for strColumn in listColumnModelAll:
            seriesScore = dfResultMonthly[indexCreateDate!=dtSelectStock].groupby(strColumn)['ReturnRelative'].mean()
            if 1 in seriesScore.index and 0 in seriesScore.index:
                score0 = seriesScore.ix[0]
                score1 = seriesScore.ix[1]
                if score1 < score0:
                    pass
                    continue
                score = score1
            else:
                continue
    
            dictModelScoreHigh[strColumn] = score1
            dictModelScoreLow[strColumn] = score0
        seriesModelScoreHigh = pandas.Series(dictModelScoreHigh)
        seriesModelScoreLow = pandas.Series(dictModelScoreLow)
        seriesModelScore = seriesModelScoreHigh

        if os.path.exists(dirTotalScore) is False:
            os.mkdir(dirTotalScore)
        seriesModelScoreHigh.to_pickle(strFileAddressHigh)
        seriesModelScoreLow.to_pickle(strFileAddressLow)
    
    listColumnModelSelected = seriesModelScore[seriesModelScore > seriesModelScore.quantile(1 - percentageModelSelected)].index.tolist()
    return listColumnModelSelected

def _filterModel_meanDiff(dfResultMonthly, dtSelectStock, percentageModelSelected, dirTotalScore=None):
    indexCreateDate = dfResultMonthly.index.get_level_values('createDate')
    
    strFileAddressHigh = dirTotalScore + '/' + 'ModelMeanReturnHigh' + dtSelectStock.strftime('%Y%m%d') + '.pickle'
    strFileAddressLow = dirTotalScore + '/' + 'ModelMeanReturnLow' + dtSelectStock.strftime('%Y%m%d') + '.pickle'
    
    if os.path.exists(strFileAddressHigh) and os.path.exists(strFileAddressLow):
        seriesModelScoreHigh = pandas.read_pickle(strFileAddressHigh)
        seriesModelScoreLow = pandas.read_pickle(strFileAddressLow)
        seriesModelScore = seriesModelScoreHigh - seriesModelScoreLow
    else:
        listColumnModelAll = []
        for strColumn in dfResultMonthly.columns:
            if strColumn.startswith('Model'):
                listColumnModelAll.append(strColumn)
        
        dictModelScoreHigh = {}
        dictModelScoreLow = {}
        for strColumn in listColumnModelAll:
            seriesScore = dfResultMonthly[indexCreateDate!=dtSelectStock].groupby(strColumn)['ReturnRelative'].mean()
            if 1 in seriesScore.index and 0 in seriesScore.index:
                score0 = seriesScore.ix[0]
                score1 = seriesScore.ix[1]
                if score1 < score0:
                    pass
                    continue
                score = score1
            else:
                continue
    
            dictModelScoreHigh[strColumn] = score1
            dictModelScoreLow[strColumn] = score0
        seriesModelScoreHigh = pandas.Series(dictModelScoreHigh)
        seriesModelScoreLow = pandas.Series(dictModelScoreLow)
        seriesModelScore = seriesModelScoreHigh - seriesModelScoreLow

        if os.path.exists(dirTotalScore) is False:
            os.mkdir(dirTotalScore)
        seriesModelScoreHigh.to_pickle(strFileAddressHigh)
        seriesModelScoreLow.to_pickle(strFileAddressLow)
    
    listColumnModelSelected = seriesModelScore[seriesModelScore > seriesModelScore.quantile(1 - percentageModelSelected)].index.tolist()
    return listColumnModelSelected

def _filterModel_cumu(dfResultMonthly, dtSelectStock, percentageModelSelected):
    #pdb.set_trace()
    indexCreateDate = dfResultMonthly.index.get_level_values('createDate')

    listColumnModelAll = []
    for strColumn in dfResultMonthly.columns:
        if strColumn.startswith('Model'):
            listColumnModelAll.append(strColumn)
    
    dictModelScore = {}
    def funcCalculate(df):
        seriesReturnRelative = df.groupby(df.index.get_level_values('createDate'))['ReturnRelative'].mean()
        returnCumulated = (seriesReturnRelative + 1).cumprod()[-1]
        #returnCumulatedAnnualized = numpy.power(returnCumulated, 1./seriesReturnRelative.index.size)
        #return returnCumulatedAnnualized
        return returnCumulated

    for strColumn in listColumnModelAll:
        seriesOneModel = dfResultMonthly.ix[indexCreateDate==dtSelectStock, strColumn]
        seriesCount = seriesOneModel.value_counts()
        NMonthValid = seriesOneModel.size
        seriesModelPerformance = dfResultMonthly.groupby(strColumn).apply(funcCalculate)
        if 1 in seriesCount.index and 0 in seriesCount.index:
            score0 = seriesModelPerformance[0]
            score1 = seriesModelPerformance[1]
            if score1 < score0:
                pass
                continue
            score = score1
        else:
            continue

        dictModelScore[strColumn] = score
        del seriesOneModel, seriesCount, seriesModelPerformance
        gc.collect()
    seriesModelScore = pandas.Series(dictModelScore)
    seriesModelScore.sort()
    listColumnModelSelected = seriesModelScore[seriesModelScore > seriesModelScore.quantile(1 - percentageModelSelected)].index.tolist()
    return listColumnModelSelected

def filterModel(dfResultMonthly, dtSelectStock, percentageModelSelected, method='mean', dirTotalScore=None):
    if method == 'mean':
        return _filterModel_mean(dfResultMonthly, dtSelectStock, percentageModelSelected, dirTotalScore=dirTotalScore)
    elif method == 'meanDiff':
        return _filterModel_meanDiff(dfResultMonthly, dtSelectStock, percentageModelSelected, dirTotalScore=dirTotalScore)
    elif method == 'cumu':
        return _filterModel_cumu(dfResultMonthly, dtSelectStock, percentageModelSelected)
    else:
        return getAllModel(dfResultMonthly)

# 选择模型 并打分
def scoreStock(dfResultMonthly, dtSelectStock, percentageModelSelected, method='mean', dirTotalScore=None):
    # 过滤模型 
    listColumnModelSelected = filterModel(dfResultMonthly, dtSelectStock, percentageModelSelected, method=method, dirTotalScore=dirTotalScore)
    indexCreateDate = dfResultMonthly.index.get_level_values('createDate')
    listColumnModelValid = listColumnModelSelected
    dfLatestMonth = dfResultMonthly[indexCreateDate==dtSelectStock].copy()
    dfLatestMonth['ScoreIntegeral'] = dfLatestMonth[listColumnModelValid].sum(axis=1)
    dfLatestMonth = dfLatestMonth.sort_values(by='ScoreIntegeral')
    return dfLatestMonth

# 筛选股票
def filterStockAfterPort(dfLatestMonthPort, dfPriceAll):
    dfPricePort = dfPriceAll[dfPriceAll['SecuCode'].isin(dfLatestMonthPort.index.get_level_values('SecuCode'))] 
    dfPricePort = dfPricePort.set_index('TradingDay').sort_index()
    dfPricePort['TurnoverVolume'] = dfPricePort['TurnoverVolume'].fillna(0) # 20161123, correct the error
    listSecuCodeValid = []
    indexTradingDayAll = dfPricePort.index.unique()
    dtSelectStock = indexTradingDayAll[0]
    dtBuyStock = indexTradingDayAll[1]
    for strSecuCode in dfLatestMonthPort.index.get_level_values('SecuCode').unique():
        # 是否停牌？
        dfPriceSecu = dfPricePort[dfPricePort['SecuCode']==strSecuCode]
        if dtSelectStock not in dfPriceSecu.index or dtBuyStock not in dfPriceSecu.index:
            continue
        if dfPriceSecu.ix[dtSelectStock, 'TurnoverVolume'] == 0 or dfPriceSecu.ix[dtBuyStock, 'TurnoverVolume'] == 0:
            continue
        # 是否开盘涨停？
        if dfPriceSecu.ix[dtBuyStock, 'ExeOpenPrice'] / dfPriceSecu.ix[dtSelectStock, 'ExeClosePrice'] - 1 > 0.09:
            continue
        listSecuCodeValid.append(strSecuCode)
    dfLatestMonthPort.ix[dfLatestMonthPort.index.get_level_values('SecuCode').isin(listSecuCodeValid), 'Operatable'] = 1
    return dfLatestMonthPort
    
# 构建portfolio
def selectPort(dfLatestMonth, NTop=50):
    dfLatestMonth = dfLatestMonth.sort_values(by='ScoreIntegeral')
    scoreLowest = dfLatestMonth.ix[-NTop]['ScoreIntegeral']
    dfLatestMonthPort = dfLatestMonth[dfLatestMonth['ScoreIntegeral'] >= scoreLowest].copy()
    #dfLatestMonthPort = dfLatestMonth[-30:]
    NStockInPort = dfLatestMonthPort.index.size
    strToLog = 'Lowest Score: {0}, Number of Stocks: {1}'.format(str(scoreLowest), NStockInPort)
    logging.log(logging.DEBUG, strToLog)
    dfLatestMonthPort['ScoreLowest'] = scoreLowest
    dfLatestMonthPort['NStockInPort'] = NStockInPort
    #print strToLog
    return dfLatestMonthPort

# 计算每日净值
def getPortDailyValue(dfLatestMonthPort, dfPriceAll):
    dfPricePort = dfPriceAll[dfPriceAll['SecuCode'].isin(dfLatestMonthPort[dfLatestMonthPort['Operatable']==1].index.get_level_values('SecuCode'))] 
    dfPricePort = dfPricePort.set_index('TradingDay').sort_index()
    dfPricePort['ReturnPCT'] = 0
    dfPricePort['ReturnCumulated'] = 0
    def funcCalculateIndividualReturn(df):
        df = df.sort_index()
        df['ReturnPCT'] = df['ExeClosePrice'].pct_change()
        df.ix[0, 'ReturnPCT'] = 0   # the first index is corresponding to the day of picking stock
        df.ix[1, 'ReturnPCT'] = df.ix[1, 'ExeClosePrice'] / df.ix[1, 'ExeOpenPrice'] - 1
        df['ReturnCumulated'] = (df['ReturnPCT'] + 1).cumprod()
        #df = df.drop('SecuCode', axis=1)
        df = df.reset_index()
        return df
    dfPricePort = dfPricePort.groupby('SecuCode').apply(funcCalculateIndividualReturn)
    dfPricePort = dfPricePort.set_index(['SecuCode', 'TradingDay']).sort_index()
    seriesPortValue = dfPricePort['ReturnCumulated'].groupby(dfPricePort.index.get_level_values('TradingDay')).sum()
    seriesPortValue = seriesPortValue.pct_change()
    seriesPortValue = seriesPortValue.ix[1:]
    return seriesPortValue

#########################
# for portfolio analysis
#########################
def getDFSecuCodeIndustryCode():
    # replace SecuCode by IndustryCode
    sql = '''
    SELECT
    DISTINCT
    SecuCode, 
    FirstIndustryCode as IndustryCode,
    FirstIndustryName as IndustryAbbr,
    InfoPublDate
    FROM baozh.dbo.ZXIndustry
    ORDER BY SecuCode, IndustryCode, InfoPublDate
    '''
    engine = connect()
    dfSecuIndustry = pandas.read_sql(sql, con=engine)
    dfSecuIndustry.to_pickle('dfSecuIndustry.pickle')
    dfSecuIndustry = dfSecuIndustry.set_index('InfoPublDate')
    listDict = []
    for SecuCode in dfSecuIndustry['SecuCode'].unique():
        dfSecu = dfSecuIndustry[dfSecuIndustry['SecuCode']==SecuCode]
        IndustryCode = dfSecu.ix[-1, 'IndustryCode']
        IndustryAbbr = dfSecu.ix[-1, 'IndustryAbbr']
        dictSecuIndustry = {
                'SecuCode': SecuCode, 
                'IndustryCode': IndustryCode,
                'IndustryName': IndustryAbbr.encode('raw-unicode-escape').decode('gbk'),
                }
        listDict.append(dictSecuIndustry)
    dfSecuCodeIndustryCode = pandas.DataFrame(listDict)
    return dfSecuCodeIndustryCode

def getIndustryReturn():
    # read industry index data
    sql = u'''
    SELECT
    DISTINCT
    a.IndustryNameZX as IndustryAbbr,
    a.IndustryCodeZX as IndustryCode,
    b.TradingDay,
    b.ExeClosePrice, 
    b.ExeOpenPrice,
    b.ExeLowPrice,
    b.ExeHighPrice,
    b.TurnoverVolume
    FROM 
    (
    	(
    	SELECT *
    	FROM baozh.dbo.TwoIndustryCodeSWZX
    	) a
    	left join 
    	(
    	SELECT *
    	FROM zhenggq.dbo.indextradingdata
    	WHERE SecuAbbr like '%中信%'
    	) b
    	on a.SecuCode = b.SecuCode
    )
    ORDER BY TradingDay
    '''
    engine = connect()
    dfTrading = pandas.read_sql(sql, con=engine)
    dfTrading.to_pickle('dfTrading.pickle')
    dfTrading['TradingDay'] = dfTrading['TradingDay'].apply(parse)
    def funcCalcDailyReturn(df):
        df = df.set_index('TradingDay')
        df = df.drop('IndustryCode', axis=1)
        df.sort_index()
        df = df.resample('1m', how='last')
        df['Return'] = df['ExeClosePrice'].pct_change().shift(-1)
        df = df.reset_index()
        return df
    dfTrading = dfTrading.groupby('IndustryCode').apply(funcCalcDailyReturn)
    dfTrading = dfTrading.reset_index().set_index(['TradingDay', 'IndustryCode'])
    return dfTrading

def getFactorRaw(dtMonth):
    if os.path.exists(dirFactorMonthlyRaw) is False:
        os.mkdir(dirFactorMonthlyRaw)
    strFileAddress = dirFactorMonthlyRaw + dtMonth.strftime('%Y%m%d') + '.pickle'
    if os.path.exists(strFileAddress):
        df = pandas.read_pickle(strFileAddress)
        return df

    sql = '''
    SELECT
    *
    FROM 
    baozh.dbo.FactorMonthly_Comparable
    WHERE
    createDate = '{0}'
    order by 
    createDate
    '''.format(
            dtMonth.strftime('%Y%m%d') 
            )
    engine = connect()
    df = pandas.read_sql(sql, engine)
    df['createDate'] = df['createDate'].apply(lambda x: parse(str(int(x))))
    df = df.reset_index().set_index(['createDate', 'SecuCode']).drop('index', axis=1)
    df.to_pickle(strFileAddress)
    return df

def assignFactor(dfPort):
    dfPort = dfPort.reset_index().set_index(['createDate', 'SecuCode'])
    listDT = dfPort.index.get_level_values('createDate').unique()
    for nFactor in range(0, len(listFactorAll)):
        listFactorAll[nFactor] = unicode(listFactorAll[nFactor])
        dfPort[listFactorAll[nFactor]] = numpy.nan
    
    listDictMean = []
    listDictMedian = []
    for dtMonth in listDT:
        print dtMonth
        dfFactor = getFactorRaw(dtMonth)
        ixCommon = dfPort.index & dfFactor.index
        dfPort.ix[ixCommon, listFactorAll] = dfFactor.ix[ixCommon, listFactorAll]
        dictMean = dfFactor[listFactorAll].mean().to_dict()
        dictMedian = dfFactor[listFactorAll].median().to_dict()
        dictMean['createDate'] = dtMonth
        dictMedian['createDate'] = dtMonth
        listDictMean.append(dictMean)
        listDictMedian.append(dictMedian)
    dfMeanMonthly = pandas.DataFrame(listDictMean).set_index('createDate')
    dfMedianMonthly = pandas.DataFrame(listDictMedian).set_index('createDate')
    dfPortMeanMonthly = dfPort[listFactorAll].reset_index().groupby('createDate').mean()
    return dfPort, dfPortMeanMonthly, dfMedianMonthly
    
def getTradingDataDaily(dt):
    if os.path.exists(dirTradingDataDaily) is False:
        os.mkdir(dirTradingDataDaily)
    strFileAddress = dirTradingDataDaily + dt.strftime('%Y%m%d') + '.pickle'
    if os.path.exists(strFileAddress):
        df = pandas.read_pickle(strFileAddress)
        return df

    dtNextMonth = dt + datetime.timedelta(1)
    sql = '''
    SELECT
    TradingDay,
    SecuCode,
    ExeClosePrice,
    ExeOpenPrice,
    TurnoverVolume,
    OpenPrice,
    ClosePrice

    FROM baozh.dbo.TradingDataDaily
    WHERE 
    (year(TradingDay) = {0}
    AND month(TradingDay) = {1})
    OR TradingDay = '{2}'
    ORDER BY TradingDay
    '''.format(
            dtNextMonth.year,
            dtNextMonth.month,
            dt.strftime('%Y%m%d')
            )
    engine = connect()
    df = pandas.read_sql(sql, engine)
    df.to_pickle(strFileAddress)
    return df

def assignTradingDataMonthly(dfPort):
    dfPort = dfPort.reset_index().set_index(['createDate', 'SecuCode'])
    listDT = dfPort.index.get_level_values('createDate').unique()
    for dtMonth in listDT:
        print dtMonth
        dfPrice = getTradingDataDaily(dtMonth)
        dfPrice = dfPrice.set_index(['TradingDay', 'SecuCode'])
        dtMonthStart = dfPrice.index.get_level_values('TradingDay').unique()[1]
        dtMonthEnd = dfPrice.index.get_level_values('TradingDay').max()
        for ix in dfPort[dfPort.index.get_level_values('createDate')==dtMonth].index:
            SecuCode = ix[1]
            ixMonthEnd = (dtMonthEnd, SecuCode)
            ixMonthStart = (dtMonthStart, SecuCode)
            dfPort.ix[ix, 'Return'] = dfPrice.ix[ixMonthEnd, 'ExeClosePrice'] / dfPrice.ix[ixMonthStart, 'ExeOpenPrice'] - 1
    
    return dfPort

def assignTradingDataDaily(dfPort):
    dfPort = dfPort.reset_index().set_index(['createDate', 'SecuCode'])
    listDT = dfPort.index.get_level_values('createDate').unique()
    seriesAll = None
    for dtMonth in listDT:
        print dtMonth
        dfPrice = getTradingDataDaily(dtMonth)
        dfPortMonthly = dfPort[dfPort.index.get_level_values('createDate')==dtMonth]
        seriesPortValue = getPortDailyValue(dfPortMonthly, dfPrice)
        if seriesAll is None:
            seriesAll = seriesPortValue
        else:
            seriesAll = seriesAll.append(seriesPortValue)
    seriesAll.name = 'Return'
    dfRet = pandas.DataFrame(seriesAll)
    return dfRet

def calculatePortValue(dfPort):
    seriesPortReturn = dfPort.reset_index().groupby(['TradingDay'])['Return'].mean()
    seriesPortValue = (seriesPortReturn+1).cumprod()
    seriesPortValue.name = u'累计净值'
    dfPortValue = pandas.DataFrame(seriesPortValue)
    seriesMax = pandas.expanding_max(dfPortValue[u'累计净值'])
    dfPortValue['MaxDD'] = (seriesMax - dfPortValue[u'累计净值']) / seriesMax
    return dfPortValue
    


# unit test
def testGetDataChunk():
    dtEnd = datetime.datetime(2010, 1, 31)
    SecuCodeIndex = '000001'
    strMethodDiscretization = 'Histogram2'
    strMethodDiscretizeY = 'two'
    NYearTrain = 3
    strTrainRandom = 'SEQ0'

    df = getDataChunk(dtEnd, SecuCodeIndex, strMethodDiscretization=strMethodDiscretization, strMethodDiscretizeY=strMethodDiscretizeY, NYearTrain=NYearTrain, strTrainRandom=strTrainRandom)
    return df

if __name__ == '__main__':
    df = testGetDataChunk()
    
