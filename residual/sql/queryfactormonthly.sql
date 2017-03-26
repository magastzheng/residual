select a.createDate
    ,a.settleDate
    ,a.TradingDay
    ,a.SecuCode
	,a.SecuAbbr
    ,a.IndustrySecuCode_I
    ,convert(float, a. ClosePrice) as ClosePrice
    ,convert(float, a.OpenPrice) as OpenPrice
    ,convert(float, a.HighPrice) as HighPrice
    ,convert(float, a.LowPrice) as LowPrice
    ,convert(float, a.ExeClosePrice) as ExeClosePrice
    ,convert(float, a.ExeOpenPrice) as ExeOpenPrice
    ,convert(float, a.ExeHighPrice) as ExeHighPrice
    ,convert(float, a.ExeLowPrice) as ExeLowPrice
    ,convert(float, a.NonRestrictedShares) as NonRestrictedShares
    ,convert(float, a.AFloats) as AFloats
    ,convert(float, a.TotalShares) as TotalShares
    ,convert(float, a.TurnoverVolume) as TurnoverVolume
    ,convert(float, a.NonRestrictedCap) as NonRestrictedCap
    ,convert(float, a.AFloatsCap) as AFloatsCap
    ,convert(float, a.TotalCap) as TotalCap
    ,convert(float, a.PE) as PE
    ,convert(float, a.PB) as PB
    ,convert(float, a.PS) as PS
    ,convert(float, a.PCF) as PCF
    ,convert(float, a.DividendYield) as DividendYield
    ,convert(float, a.DividendRatio) as DividendRatio
    ,convert(float, a.TTMIncome) as TTMIncome
    ,convert(float, a.GP_Margin) as GP_Margin
    ,convert(float, a.NP_Margin) as NP_Margin
    ,convert(float, a.ROA) as ROA
    ,convert(float, a.ROE) as ROE
    ,convert(float, a.AssetsTurnover) as AssetsTurnover
    ,convert(float, a.EquityTurnover) as EquityTurnover
    ,convert(float, a.Cash_to_Assets) as Cash_to_Assets
    ,convert(float, a.Liability_to_Assets) as Liability_to_Assets
    ,convert(float, a.EquityMultiplier) as EquityMultiplier
    ,convert(float, a.CurrentRatio) as CurrentRatio
    ,convert(float, a.Income_Growth_YOY_Comparable) as Income_Growth_YOY_Comparable
    ,convert(float, a.NPPC_Growth_YOY_Comparable) as NPPC_Growth_YOY_Comparable
    ,convert(float, a.GP_Margin_Comparable) as GP_Margin_Comparable
    ,convert(float, a.GP_Margin_Growth_YOY_Comparable) as GP_Margin_Growth_YOY_Comparable
    ,convert(float, a.NP_Margin_Comparable) as NP_Margin_Comparable
    ,convert(float, a.NP_Margin_Growth_YOY_Comparable) as NP_Margin_Growth_YOY_Comparable
    ,convert(float, a.Income_Growth_Pre_Comparable) as Income_Growth_Pre_Comparable
    ,convert(float, a.NPPC_Growth_Pre_Comparable) as NPPC_Growth_Pre_Comparable
    ,convert(float, a.GP_Margin_Growth_Pre_Comparable) as GP_Margin_Growth_Pre_Comparable
    ,convert(float, a.NP_Margin_Growth_Pre_Comparable) as NP_Margin_Growth_Pre_Comparable
    ,convert(float, a.NPPC_Growth_Pre_Season) as NPPC_Growth_Pre_Season
    ,convert(float, a.NPPC_Growth_YOY_Season) as NPPC_Growth_YOY_Season
    ,convert(float, a.NPLNRP_Growth_Pre_Season) as NPLNRP_Growth_Pre_Season
    ,convert(float, a.NPLNRP_Growth_YOY_Season) as NPLNRP_Growth_YOY_Season
    ,convert(float, a.Income_Growth_Pre_Season) as Income_Growth_Pre_Season
    ,convert(float, a.Income_Growth_YOY_Season) as Income_Growth_YOY_Season
    ,convert(float, a.Income_Growth_Qtr_Comparable) as Income_Growth_Qtr_Comparable
    ,convert(float, a.NPPC_Growth_Qtr_Comparable) as NPPC_Growth_Qtr_Comparable
    ,convert(float, a.GP_Margin_Qtr) as GP_Margin_Qtr
    ,convert(float, a.GP_Margin_Growth_Qtr_Comparable) as GP_Margin_Growth_Qtr_Comparable
    ,convert(float, a.IPS_Qtr) as IPS_Qtr
    ,convert(float, a.EPS_Qtr) as EPS_Qtr
    ,convert(float, a.ROE_Qtr) as ROE_Qtr
    ,convert(float, a.Income_Growth_Pre) as Income_Growth_Pre
    ,convert(float, a.NPPC_Growth_Pre) as NPPC_Growth_Pre
    ,convert(float, a.NPLNRP_Growth_Pre) as NPLNRP_Growth_Pre
    ,convert(float, a.GP_Margin_Growth_Pre) as GP_Margin_Growth_Pre
    ,convert(float, a.NP_Margin_Growth_Pre) as NP_Margin_Growth_Pre
    ,convert(float, a.Income_Growth_YOY) as Income_Growth_YOY
    ,convert(float, a.NPPC_Growth_YOY) as NPPC_Growth_YOY
    ,convert(float, a.NPLNRP_Growth_YOY) as NPLNRP_Growth_YOY
    ,convert(float, a.GP_Margin_Growth_YOY) as GP_Margin_Growth_YOY
    ,convert(float, a.NP_Margin_Growth_YOY) as NP_Margin_Growth_YOY
    ,convert(float, a.IPS) as IPS
    ,convert(float, a.EPS) as EPS
    ,convert(float, a.CFPS) as CFPS
    ,convert(float, a.Pre_IPS) as Pre_IPS
    ,convert(float, a.Pre_EPS) as Pre_EPS
    ,convert(float, a.Pre_CFPS) as Pre_CFPS
    ,convert(float, a.YOY_IPS) as YOY_IPS
    ,convert(float, a.YOY_EPS) as YOY_EPS
    ,convert(float, a.YOY_CFPS) as YOY_CFPS
    ,convert(float, a.rPE) as rPE
    ,convert(float, a.rPB) as rPB
    ,convert(float, a.rPS) as rPS
    ,convert(float, a.rPCF) as rPCF
    ,convert(float, a.SettlePrice) as SettlePrice
    ,convert(float, a.IndusInnerCode) as IndusInnerCode
    ,convert(float, a.IndusCreatePrice) as IndusCreatePrice
    ,convert(float, a.IndusSettlePrice) as IndusSettlePrice
    ,convert(float, a.ExecutivesProp) as ExecutivesProp
    ,convert(float, a.InstitutionNum) as InstitutionNum
    ,convert(float, a.InstitutionProp) as InstitutionProp
    ,convert(float, a.RegionScore) as RegionScore
    ,convert(float, a.ExeClosePrice_CreateDate_Wind) as ExeClosePrice_CreateDate_Wind
    ,convert(float, a.ExeClosePrice_SettleDate_Wind) as ExeClosePrice_SettleDate_Wind
    ,convert(float, a.Momentum20Day) as Momentum20Day
    ,convert(float, a.Momentum40Day) as Momentum40Day
    ,convert(float, a.Momentum60Day) as Momentum60Day
    ,convert(float, a.Momentum120Day) as Momentum120Day
    ,convert(float, a.Momentum180Day) as Momentum180Day
    ,convert(float, a.Momentum240Day) as Momentum240Day
    ,convert(float, a.PriceDiff) as PriceDiff
    ,convert(float, a.DayDiff) as DayDiff
    ,convert(float, a.TurnoverRatio) as TurnoverRatio
    ,convert(float, a.AvgTurnoverPrice) as AvgTurnoverPrice
    ,convert(float, a.AvgTurnoverPriceFactor) as AvgTurnoverPriceFactor
    ,convert(float, a.AvgTurnoverRatio5Day) as AvgTurnoverRatio5Day
    ,convert(float, a.AvgTurnoverRatio10Day) as AvgTurnoverRatio10Day
    ,convert(float, a.AvgTurnoverRatio20Day) as AvgTurnoverRatio20Day
    ,convert(float, a.AvgTurnoverRatio40Day) as AvgTurnoverRatio40Day
    ,convert(float, a.AvgTurnoverRatio60Day) as AvgTurnoverRatio60Day
    ,convert(float, a.AvgTurnoverRatio120Day) as AvgTurnoverRatio120Day
    ,convert(float, a.E_EPS) as E_EPS
    ,convert(float, a.E_growth) as E_growth
    ,convert(float, a.FreeFloat_Total) as FreeFloat_Total
    ,convert(float, a.EPS_CFPS) as EPS_CFPS
    ,convert(float, a.AvgTurnoverValue) as AvgTurnoverValue
from baozh.dbo.FactorMonthly_Comparable a
where a.TradingDay='{0}'



