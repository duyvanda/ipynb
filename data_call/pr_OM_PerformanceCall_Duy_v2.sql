USE [PhaNam_eSales_PRO]
GO

/****** Object:  StoredProcedure [dbo].[pr_OM_PerformanceCall]    Script Date: 26/10/2021 10:48:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[pr_OM_PerformanceCall] -- pr_OM_PerformanceCall '20211008','20211008'
@Fromdate DATE,
@Todate DATE
AS
SET NOCOUNT ON;
/*
Lấy ra các trường hợp check-in giao hàng và thu nợ
*/
SELECT BranchID,
NumberCICO
INTO #WithOutNumberCICO
FROM dbo.AR_SalespersonLocationTrace WITH (NOLOCK)
WHERE Type LIKE '%DE%'
OR Type LIKE '%PA%'
AND CAST(UpdateTime AS DATE)
BETWEEN @Fromdate AND @Todate;

/*
Lấy ra danh sách KH MCP theo thời gian, giảm tải join trực tiếp
*/
SELECT *
INTO #T_RouteDet
FROM dbo.OM_SalesRouteDet d WITH (NOLOCK)
WHERE d.VisitDate
BETWEEN @Fromdate AND @Todate;

/*
Lấy danh sách khách hàng ngoại tuyến, giảm tải join trực tiếp
*/
SELECT DISTINCT
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
SELECT BranchID,
SlsperID,
SupID,
ASM,
RSMID
INTO #TSlsperID
FROM dbo.fr_ListSaleByData('Admin');


--/* -----------------------------Main Query--------------------*/
SELECT K.BranchID,
K.SlsperID,
K.SalesRouteID,
--K.SlsFreq,
K.CustID,
K.VisitDate,
InRoute = MAX(K.InRoute),
ExtRoute = MAX(K.ExtRoute),
Visited = MAX(K.Visited),
OrderFromPDA = MAX(K.OrderFromPDA),
OrdAmtFromPDA = MAX(OrdAmtFromPDA),
OrderFromOther = MAX(OrderFromOther),
OrdAmtFromOther = MAX(OrdAmtFromOther)
INTO #TResult
FROM
(
/*Kế hoạch MCP, Danh sách kế hoạch trong tuyến*/
    SELECT 
    srm.BranchID,
    srm.SlsperID,
    rd.SalesRouteID,
    --rd.SlsFreq,
    rd.CustID,
    rd.VisitDate,
    InRoute = 1,
    ExtRoute = 0,
    Visited = 0,
    OrderFromPDA = 0,
    OrdAmtFromPDA = 0,
    OrderFromOther = 0,
    OrdAmtFromOther = 0
    FROM dbo.#T_RouteDet rd WITH (NOLOCK)
    INNER JOIN dbo.Vs_SalespersonRoute srm WITH (NOLOCK)
    ON rd.SalesRouteID = srm.SalesRouteID
    AND rd.VisitDate
    BETWEEN srm.FromDate AND srm.ToDate
    WHERE rd.DayofWeek <> 'Sun'
    UNION ALL

    /*Thực tế viếng thăm: 
    1.Khách hàng trong hoặc trái tuyến: ExtRoute=0
    2.Khách hàng ngoại tuyến: ExtRoute=1
    */
    SELECT DISTINCT
    -- #TResult 2
    slt.BranchID,
    slt.SlsperID,
    srm.SalesRouteID,
    --rod.SlsFreq,
    slt.CustID,
    VisitDate = CAST(slt.UpdateTime AS DATE),
    InRoute = 0,
    ExtRoute = CASE WHEN et.SlsperID IS NOT NULL THEN 1 ELSE 0 END,
    Visited = 1,
    OrderFromPDA = 0,
    OrdAmtFromPDA = 0,
    OrderFromOther = 0,
    OrdAmtFromOther = 0
    FROM dbo.AR_SalespersonLocationTrace slt WITH (NOLOCK)

    INNER JOIN dbo.Vs_SalespersonRoute srm WITH (NOLOCK)
    ON srm.BranchID = slt.BranchID
    AND srm.SlsperID = slt.SlsperID
    AND CAST(slt.UpdateTime AS DATE) BETWEEN srm.FromDate AND srm.ToDate

    LEFT JOIN #T_ExtRoute et WITH (NOLOCK)
    ON slt.BranchID = et.BranchID
    AND et.CustID = slt.CustID
    AND et.SlsperID = slt.SlsPerID
    AND CAST(slt.UpdateTime AS DATE) BETWEEN et.StartDate AND et.EndDate

    LEFT JOIN #WithOutNumberCICO wcc WITH (NOLOCK)
    ON wcc.BranchID = slt.BranchID
    AND wcc.NumberCICO = slt.NumberCICO
    WHERE wcc.NumberCICO IS NULL
    AND CAST(slt.UpdateTime AS DATE) BETWEEN @Fromdate AND @Todate

    UNION ALL
    -- #TResult 3
    /*Thông tin đặt hàng trên PDA hoặc Cloud, các đơn hàng phát sinh phải được duyệt mới được tính số*/
    SELECT DISTINCT
    o.BranchID,
    o.SlsPerID,
    o.SalesRouteID,
    --m.SlsFreq,
    o.CustID,
    VisitDate = CAST(o.Crtd_DateTime AS DATE),
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
    AND CAST(o.Crtd_DateTime AS DATE) BETWEEN et.StartDate AND et.EndDate

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
    --m.SlsFreq
) K
GROUP BY K.BranchID,
K.SlsperID,
K.SalesRouteID,
--K.SlsFreq,
K.CustID,
K.VisitDate;


SELECT 
r.BranchID,
c.CpnyName,
r.SlsperID,
SlsperName = u.FirstName,
Position = CASE
WHEN u.Position IN ( 'S', 'SS', 'AM', 'RM' ) THEN 'P.BH'
WHEN u.Position IN ( 'D', 'SD', 'AD', 'RD' ) THEN 'MDS'
ELSE
u.Position
END,
SupName = sup.FirstName,
ASMName = asm.FirstName,
RSMName = rsm.FirstName,
r.CustID,
cu.CustName,
-- r.SlsFreq,
cu.Channel,
cu.ShopType,
cu.HCOID,
cu.HCOTypeID,
cu.ClassId,
cu.TerritoryDescr,
cu.StateDescr,
cu.DistrictDescr,
cu.WardDescr,
r.VisitDate,
r.InRoute,
r.ExtRoute,
r.Visited,
r.OrderFromPDA,
r.OrdAmtFromPDA,
r.OrderFromOther,
r.OrdAmtFromOther
FROM #TResult r
INNER JOIN dbo.Users u
ON u.UserName = r.SlsperID

INNER JOIN dbo.vs_AR_CustomerInfo cu WITH (NOLOCK)
ON cu.CustId = r.CustID

LEFT JOIN #TSlsperID ts WITH (NOLOCK)
ON ts.BranchID = r.BranchID
AND ts.SlsperID = r.SlsperID

LEFT JOIN dbo.Users sup WITH (NOLOCK)
ON sup.UserName = ts.SupID

LEFT JOIN dbo.Users asm WITH (NOLOCK)
ON asm.UserName = ts.ASM

LEFT JOIN dbo.Users rsm WITH (NOLOCK)
ON rsm.UserName = ts.RSMID

INNER JOIN dbo.SYS_Company c
ON r.BranchID = c.CpnyID;


DROP TABLE #WithOutNumberCICO;
DROP TABLE #T_RouteDet;
DROP TABLE #T_ExtRoute;
DROP TABLE #TSlsperID;
DROP TABLE #TResult;
GO