select a.* 
from advancedb.dbo.FactorDaily_Comparable a
join zhenggq.dbo.securitybasicinfo b
on a.SecuCode=b.SecuCode
where a.TradingDay>dateadd(year, 1, b.IPODay) 
and a.TradingDay='%s'
