-- Route Master = all routes CustID và SalesRouteID
-- RouteDet Tự chia theo lịch của Route Master
-- Vs_SalespersonRoute gán CustID và SalesRouteID cho SlsperID
-- AR_SalespersonLocationTrace detail vieng tham ( loai type DE PA)
-- PDA Sales Ord detail sales, pso.InsertFrom = 'S' thì đơn là từ PDA ngược lại là CS

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[pr_OM_PerformanceCall] -- pr_OM_PerformanceCall '20211001','20211020'
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
SELECT 
BranchID,
SlsperID,
SupID,
ASM,
RSMID
INTO #TSlsperID
FROM dbo.fr_ListSaleByData('Admin');


--/* -----------------------------Main Query--------------------*/
SELECT 
K.BranchID,
K.SlsperID,
K.SalesRouteID,
K.SlsFreq,
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
    -- #TResult 1
    SELECT 
    srm.BranchID,
    srm.SlsperID,
    rd.SalesRouteID,
    rd.SlsFreq,
    rd.CustID,
    rd.VisitDate,
    InRoute = 1,
    ExtRoute = 0,
    Visited = 0,
    OrderFromPDA = 0,
    OrdAmtFromPDA = 0,
    OrderFromOther = 0,
    OrdAmtFromOther = 0
    FROM #T_RouteDet rd WITH (NOLOCK)
    INNER JOIN dbo.Vs_SalespersonRoute srm WITH (NOLOCK) ON
    rd.SalesRouteID = srm.SalesRouteID AND
    rd.VisitDate BETWEEN srm.FromDate AND srm.ToDate
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
    rd.SalesRouteID,
    rd.SlsFreq,
    slt.CustID,
    VisitDate = CAST(slt.UpdateTime AS DATE),
    InRoute = 0,
    ExtRoute = CASE WHEN rd.SalesRouteID IS NOT NULL THEN 1 ELSE 0 END,
    Visited = 1,
    OrderFromPDA = 0,
    OrdAmtFromPDA = 0,
    OrderFromOther = 0,
    OrdAmtFromOther = 0
    FROM dbo.AR_SalespersonLocationTrace slt WITH (NOLOCK)

    INNER JOIN dbo.Vs_SalespersonRoute srm WITH (NOLOCK) ON
    srm.BranchID = slt.BranchID AND
    srm.SlsperID = slt.SlsperID
    AND CAST(slt.UpdateTime AS DATE) BETWEEN srm.FromDate AND srm.ToDate
            
    LEFT JOIN #T_RouteDet rd WITH (NOLOCK) ON 
    rd.CustID = slt.CustID AND
    rd.DayofWeek = 'Sun' AND
    rd.SalesRouteID = srm.SalesRouteID AND
    rd.VisitDate BETWEEN srm.FromDate AND srm.ToDate

    LEFT JOIN #WithOutNumberCICO wcc WITH (NOLOCK) ON 
    wcc.BranchID = slt.BranchID AND
    wcc.NumberCICO = slt.NumberCICO
    WHERE wcc.NumberCICO IS NULL
    AND CAST(slt.UpdateTime AS DATE) BETWEEN @Fromdate AND @Todate

    UNION ALL
    /*Thông tin đặt hàng trên PDA hoặc Cloud, các đơn hàng phát sinh phải được duyệt mới được tính số*/
    -- #TResult 3
    SELECT 
    -- DISTINCT
    pso.BranchID,
    pso.SlsPerID,
    pso.SalesRouteID,
    slr.SlsFreq,
    pso.CustID,
    VisitDate = CAST(pso.Crtd_DateTime AS DATE),
    InRoute = 0,
    ExtRoute = CASE WHEN et.BranchID IS NOT NULL THEN 1 ELSE 0 END,
    Visited = 0,
    OrderFromPDA = CASE WHEN pso.InsertFrom = 'S' THEN 1 ELSE 0 END,
    OrdAmtFromPDA = SUM(CASE WHEN pso.InsertFrom = 'S' THEN pso.LineAmt ELSE 0 END),
    OrderFromOther = CASE WHEN pso.InsertFrom <> 'S' THEN 1 ELSE 0 END,
    OrdAmtFromOther = SUM(CASE WHEN pso.InsertFrom <> 'S' THEN pso.LineAmt ELSE 0 END)
    FROM dbo.OM_PDASalesOrd pso WITH (NOLOCK)

    LEFT JOIN #T_ExtRoute et WITH (NOLOCK) ON  
    pso.BranchID = et.BranchID AND
    et.CustID = pso.CustID AND
    et.SlsperID = pso.SlsPerID AND
    CAST(pso.Crtd_DateTime AS DATE) BETWEEN et.StartDate AND et.EndDate

    INNER JOIN dbo.vs_OM_SalesRouteMaster slr WITH (NOLOCK) ON
    slr.CustID = pso.CustID
    AND CAST(pso.Crtd_DateTime AS DATE) BETWEEN slr.StartDate AND slr.EndDate

    WHERE CAST(pso.Crtd_DateTime AS DATE) BETWEEN @Fromdate AND @Todate AND 
    pso.Status = 'C' AND
    pso.OrderType = 'IN'

    GROUP BY CAST(pso.Crtd_DateTime AS DATE),
    CASE WHEN et.BranchID IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN pso.InsertFrom = 'S' THEN 1 ELSE 0 END,
    CASE WHEN pso.InsertFrom <> 'S' THEN 1 ELSE 0 END,
    pso.BranchID,
    pso.SlsPerID,
    pso.SalesRouteID,
    pso.CustID,
    slr.SlsFreq
    ) K
GROUP BY K.BranchID,
K.SlsperID,
K.SalesRouteID,
K.SlsFreq,
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
r.SlsFreq,
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
