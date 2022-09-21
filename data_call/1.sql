-- SELECT CustId, CustName, Channel, ShopType, HCOID, HCOTypeID, ClassId, TerritoryDescr, StateDescr, DistrictDescr, WardDescr from dbo.vs_AR_CustomerInfo

SELECT TOP 1
CustID, DayofWeek, SalesRouteID, VisitDate, 'SlsFreq'
--INTO #T_RouteDet
FROM dbo.OM_SalesRouteDet d WITH (NOLOCK)

DROP TABLE IF EXISTS #WithOutNumberCICO
DROP TABLE IF EXISTS #T_RouteDet
DROP TABLE IF EXISTS #T_ExtRoute;
DROP TABLE IF EXISTS #TSlsperID;
DROP TABLE IF EXISTS #TResult;

DECLARE  @FromDate DATE, @ToDate DATE
SELECT @FromDate='20210901', @ToDate='20211001'

SELECT BranchID,
NumberCICO
INTO #WithOutNumberCICO
FROM dbo.AR_SalespersonLocationTrace WITH (NOLOCK)
-- Giao hang DE
WHERE Type LIKE '%DE%'
OR Type LIKE '%PA%' -- PA thu no
AND CAST(UpdateTime AS DATE)
BETWEEN @Fromdate AND @Todate;

/*
Lấy ra danh sách KH MCP theo thời gian, giảm tải join trực tiếp
*/
SELECT *
-- CustID, DayofWeek, SalesRouteID, VisitDate
INTO #T_RouteDet
FROM dbo.OM_SalesRouteDet d WITH (NOLOCK)
WHERE d.VisitDate
BETWEEN @Fromdate AND @Todate;

/*
Lấy danh sách khách hàng ngoại tuyến, giảm tải join trực tiếp
*/
SELECT 
DISTINCT
srm.BranchID,
rd.CustID,
srm.SlsperID,
rd.SlsFreq,
StartDate = slr.StartDate,
EndDate = slr.EndDate
INTO #T_ExtRoute
FROM dbo.Vs_SalespersonRoute srm WITH (NOLOCK)
INNER JOIN #T_RouteDet rd WITH (NOLOCK)
ON rd.SalesRouteID = srm.SalesRouteID
AND rd.DayofWeek = 'Sun'
AND rd.VisitDate
BETWEEN srm.FromDate AND srm.ToDate

INNER JOIN dbo.vs_OM_SalesRouteMaster slr WITH (NOLOCK)
ON slr.CustID = rd.CustID
AND slr.SalesRouteID = rd.SalesRouteID
AND rd.VisitDate
BETWEEN slr.StartDate AND slr.EndDate;
/*Danh sách cấu trúc salesforce - Slsper-Sup */


SELECT count(*) from #T_ExtRoute

--/* -----------------------------Main Query--------------------*/

SELECT 
-- count(*)
sum(ExtRoute)
from (
    SELECT
    o.BranchID,
    o.SlsPerID,
    o.SalesRouteID,
    --m.SlsFreq,
    o.CustID,
    VisitDate = CAST(o.Crtd_DateTime AS DATE),
    -- et.StartDate,
    -- et.EndDate,
    InRoute = 0,
    ExtRoute = CASE WHEN et.BranchID IS NOT NULL THEN 1 ELSE 0 END,
    Visited = 0,
    OrderFromPDA = CASE WHEN o.InsertFrom = 'S' THEN 1 ELSE 0 END,
    OrdAmtFromPDA = SUM(CASE WHEN o.InsertFrom = 'S' THEN o.LineAmt ELSE 0 END),
    OrderFromOther = CASE WHEN o.InsertFrom <> 'S' THEN 1 ELSE 0 END,
    OrdAmtFromOther = SUM(CASE WHEN o.InsertFrom <> 'S' THEN o.LineAmt ELSE 0 END)
    FROM dbo.OM_PDASalesOrd o WITH (NOLOCK)

    INNER JOIN dbo.vs_OM_SalesRouteMaster m WITH (NOLOCK)
    ON m.CustID = o.CustID
    AND CAST(o.Crtd_DateTime AS DATE) BETWEEN m.StartDate AND m.EndDate

    LEFT JOIN #T_ExtRoute et WITH (NOLOCK)
    ON o.BranchID = et.BranchID
    AND et.CustID = o.CustID
    AND et.SlsperID = o.SlsPerID
    -- AND CAST(o.Crtd_DateTime AS DATE) BETWEEN et.StartDate AND et.EndDate

    WHERE CAST(o.Crtd_DateTime AS DATE) BETWEEN @Fromdate AND @Todate
    AND o.Status = 'C'
    AND o.OrderType = 'IN'

    GROUP BY CAST(o.Crtd_DateTime AS DATE),
    CASE WHEN et.BranchID IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN o.InsertFrom = 'S' THEN 1 ELSE 0 END,
    CASE WHEN o.InsertFrom <> 'S' THEN 1 ELSE 0 END,
    o.BranchID,
    o.SlsPerID,
    o.SalesRouteID,
    o.CustID
    -- m.SlsFreq
    ) as A