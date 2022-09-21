DECLARE @UserID VARCHAR(30) = 'Admin';
DECLARE @Terr VARCHAR(MAX) = '';
DECLARE @Zone VARCHAR(MAX) = '';
DECLARE @FromDate VARCHAR(30) = '20210501';
DECLARE @ToDate VARCHAR(30) = '20210530';

DROP TABLE IF EXISTS #DataReturnIO
DECLARE @CurrentDate DATE = CAST(GETDATE() AS DATE);


DROP TABLE IF EXISTS #DataReturnIO
SELECT o.BranchID,
o.OrderNbr
INTO #DataReturnIO
FROM OM_SalesOrd o WITH (NOLOCK)
INNER JOIN OM_SalesOrd o1 WITH (NOLOCK)
ON o.BranchID = o1.BranchID
AND o.InvcNbr = o1.InvcNbr
AND o.InvcNote = o1.InvcNote
AND o1.OrderType = 'IO'
WHERE o.OrderDate
BETWEEN '20210501' AND '20210930'
AND o.OrderDate >= '20210501'
AND o.OrderType IN ( 'CO', 'EP' )
AND o.Status = 'C';

DROP TABLE IF EXISTS #TOrderType
CREATE TABLE #TOrderType
(
OrderType VARCHAR(5),
Descr NVARCHAR(100)
);
INSERT INTO #TOrderType
EXEC pr_ListOrderType_DS @ReportNbr = N'OLAP106',
@ReportDate = @CurrentDate,
@DateParm00 = @CurrentDate,
@DateParm01 = @CurrentDate,
@DateParm02 = @CurrentDate,
@DateParm03 = @CurrentDate,
@BooleanParm00 = N'0',
@BooleanParm01 = N'0',
@BooleanParm02 = N'0',
@BooleanParm03 = N'0',
@StringParm00 = N'',
@StringParm01 = N'',
@StringParm02 = N'',
@StringParm03 = N'',
@UserID = @UserID,
@CpnyID = N'MR0001',
@LangID = N'1';

DROP TABLE IF EXISTS #TCpnyID
CREATE TABLE #TCpnyID
(
CpnyID VARCHAR(30),
Company NVARCHAR(200),
Status NVARCHAR(50),
Addres NVARCHAR(500),
Tel NVARCHAR(20)
);
INSERT INTO #TCpnyID
EXEC pr_ListCompanyByTerr @ReportNbr = N'OLAP106',
@ReportDate = @CurrentDate,
@DateParm00 = @CurrentDate,
@DateParm01 = @CurrentDate,
@DateParm02 = @CurrentDate,
@DateParm03 = @CurrentDate,
@BooleanParm00 = N'0',
@BooleanParm01 = N'0',
@BooleanParm02 = N'0',
@BooleanParm03 = N'0',
@StringParm00 = N'',
@StringParm01 = N'',
@StringParm02 = N'',
@StringParm03 = N'',
@UserID = @UserID,
@CpnyID = N'MR0001',
@LangID = N'1';

DROP TABLE IF EXISTS #Ord
SELECT BranchID,
       OrderNbr,
       MaCT,
       SlsperID,
       OrderDate,
       ReturnOrder,
       ReturnOrderdate,
       InvtID,
       Lotsernbr,
       ExpDate,
       Status,
       CustID,
       VATAmount,
       BeforeVATAmount,
       AfterVATAmount,
       Crtd_User,
       Crtd_DateTime,
       ContractID,
       DeliveryID,
       ShipDate,
       OrdAmt,
       OrdQty,
       InvcNbr,
       InvcNote,
       ChietKhau,
       a.OrderType,
       ContractNbr,
       SlsPrice,
       BeforeVATPrice,
       FreeItem,
       LineRef,
       ReasonCode, 
	   a.SupID,
	   a.ASM,
	   a.RSM  
INTO #Ord
FROM
(
SELECT so.BranchID,
CASE
    WHEN so.OrigOrderNbr <> '' THEN so.OrigOrderNbr
    ELSE so.OrderNbr
END AS OrderNbr,
MaCT = so.OrderNbr,
so.SlsperID,
so.OrderDate,
ReturnOrder = ISNULL(a1.OrigOrderNbr, ''),
ReturnOrderdate = ISNULL(a1.OrderDate, '19000101'),
CASE
    WHEN ISNULL(so.status, '') = '' THEN
        (
        CASE
            WHEN a.Status = 'C' THEN
                N'Đã Duyệt Đơn Hàng'
            WHEN a.Status = 'H' THEN
                N'Chờ Xử Lý'
            WHEN a.Status = 'E' THEN
                N'Đóng Đơn Hàng'
            WHEN a.Status = 'V' THEN
                N'Hủy Đơn Hàng'
        END
        )
    ELSE
            (
            CASE
                WHEN so.status = 'C' THEN
                    N'Đã Phát Hành'
                WHEN so.status = 'I' THEN
                    N'Tạo Hóa Đơn'
                WHEN so.status = 'N' THEN
                    N'Tạo Hóa Đơn'
                WHEN so.status = 'H' THEN
                    N'Chờ Xử Lý'
                WHEN so.status = 'E' THEN
                    N'Đóng Đơn Hàng'
                WHEN so.status = 'V' THEN
                    N'Hủy Hóa Đơn'
            END
            )
    END AS status,
so.CustID,
so.InvtID,
Lotsernbr = so.Lotsernbr,
ExpDate = so.ExpDate,
VATAmount = SUM(so.VATAmount),
BeforeVATAmount = SUM(so.BeforeVATAmount),
AfterVATAmount = SUM(so.AfterVATAmount),
so.Crtd_User,
so.Crtd_DateTime,
so.ContractID,
so.DeliveryID,
so.ShipDate,
OrdAmt = ISNULL(so.OrdAmt, 0),
OrdQty = so.Qty,
InvcNbr = ISNULL(so.InvcNbr, ''),
InvcNote = ISNULL(so.InvcNote, ''),
so.LineRef,
ChietKhau = SUM(so.ChietKhau),
so.OrderType,
ContractNbr = ISNULL(ctr.ContractNbr, ''),
so.SlsPrice,
so.BeforeVATPrice,
so.FreeItem,
CASE
    WHEN a.ReasonCode <> '' THEN a.ReasonCode
    ELSE so.ReasonCode
END AS ReasonCode,
so.SupID,
so.ASM,
so.RSM
FROM
    (
    SELECT DISTINCT
    o.BranchID,
    o.OrderDate,
    o.CustID,
    OrigOrderNbr = o.OrigOrderNbr,
    o.OrderNbr,
    o.Crtd_User,
    o.Crtd_DateTime,
    status = MIN(o.Status),
    o.ContractID,
    o.OrdAmt,
    o.InvcNbr,
    o.InvcNote,
    b.SlsperID,
    b.InvtID,
    b.FreeItem,
    Qty = SUM(ISNULL(l.Qty, b.LineQty)),
    Lotsernbr = ISNULL(l.LotSerNbr, ''),
    a.DeliveryID,
    a.ShipDate,
    ExpDate = CAST(ISNULL(l.ExpDate, '') AS VARCHAR(20)),
    ChietKhau = (o.OrdDiscAmt + o.VolDiscAmt),
    SUM(
    CASE
        WHEN b.FreeItem = 1 THEN 0
        ELSE
            (CASE
                    WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
                    WHEN oo.ARDocType IN ( 'NA' ) THEN 0
                    ELSE -1
            END) * b.BeforeVATAmount
        END
    ) AS BeforeVATAmount,

    SUM(
    CASE
        WHEN b.FreeItem = 1 THEN 0
        ELSE
            (CASE
                WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
                WHEN oo.ARDocType IN ( 'NA' ) THEN 0
                ELSE -1
            END ) * b.AfterVATAmount
    END
    ) AS AfterVATAmount,

    SUM(
    CASE
        WHEN b.FreeItem = 1 THEN 0
        ELSE
            (CASE
                WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
                WHEN oo.ARDocType IN ( 'NA' ) THEN 0
                ELSE -1
            END) * b.VATAmount
    END
    ) AS VATAmount,
    b.SlsPrice,
    BeforeVATPrice = ROUND(b.BeforeVATPrice, 0),
    o.OrderType,
    ReasonCode = o.ReasonCode,
    b.LineRef,
    b.SupID,
    b.ASM,
    b.RSM
    FROM dbo.OM_SalesOrd o WITH (NOLOCK)
    INNER JOIN OM_SalesOrdDet b WITH (NOLOCK) ON 
    o.BranchID = b.BranchID AND
    o.OrderNbr = b.OrderNbr
    LEFT JOIN OM_LotTrans l WITH (NOLOCK) ON
    l.BranchID = b.BranchID AND
    l.OrderNbr = b.OrderNbr AND
    l.OMLineRef = b.LineRef
    LEFT JOIN #DataReturnIO oity ON
    oity.BranchID = o.BranchID AND
    oity.OrderNbr = o.OrderNbr
    INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK) ON
    oo.OrderType = o.OrderType
    AND ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
    INNER JOIN #TOrderType r1 WITH (NOLOCK) ON
    r1.OrderType = oo.OrderType
    INNER JOIN #TCpnyID r WITH (NOLOCK) ON
    r.CpnyID = o.BranchID
    LEFT JOIN dbo.OM_PDASalesOrd a WITH (NOLOCK) ON
    o.BranchID = a.BranchID AND
    o.OrigOrderNbr = a.OrderNbr
    WHERE (o.Status = 'C')
    AND CAST(o.OrderDate AS DATE)
    BETWEEN @FromDate AND @ToDate
    AND o.SalesOrderType <> 'RP' --  and o.invcnbr='0086713'
    AND oity.OrderNbr IS NULL
    GROUP BY ISNULL(l.LotSerNbr, ''),
    CAST(ISNULL(l.ExpDate, '') AS VARCHAR(20)),
    (o.OrdDiscAmt + o.VolDiscAmt),
    ROUND(b.BeforeVATPrice, 0),
    o.BranchID,
    o.OrderDate,
    o.CustID,
    o.OrigOrderNbr,
    o.OrderNbr,
    o.Crtd_User,
    o.Crtd_DateTime,
    o.ContractID,
    o.OrdAmt,
    o.InvcNbr,
    o.InvcNote,
    b.SlsperID,
    b.InvtID,
    b.FreeItem,
    a.DeliveryID,
    a.ShipDate,
    b.SlsPrice,
    o.OrderType,
    o.ReasonCode,
    b.LineRef,
    b.SupID,
    b.ASM,
    b.RSM
    UNION ALL
    SELECT DISTINCT
    o.BranchID,
    a.OrderDate,
    o.CustID,
    OrigOrderNbr = a.OrigOrderNbr,
    o.OrderNbr,
    o.Crtd_User,
    o.Crtd_DateTime,
    status = MIN(o.Status),
    o.ContractID,
    o.OrdAmt,
    o.InvcNbr,
    o.InvcNote,
    b.SlsperID,
    b.InvtID,
    b.FreeItem,
    Qty = SUM(ISNULL(l.Qty, b.LineQty)),
    Lotsernbr = ISNULL(l.LotSerNbr, ''),
    a.DeliveryID,
    a.ShipDate,
    ExpDate = CAST(ISNULL(l.ExpDate, '') AS VARCHAR(20)),
    ChietKhau = (o.OrdDiscAmt + o.VolDiscAmt),
    SUM(
    CASE
        WHEN b.FreeItem = 1 THEN 0
        ELSE
            (CASE
                    WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
                    WHEN oo.ARDocType IN ( 'NA' ) THEN 0
                    ELSE -1
            END) * b.BeforeVATAmount
        END
    ) AS BeforeVATAmount,

    SUM(
    CASE
        WHEN b.FreeItem = 1 THEN 0
        ELSE
            (CASE
                WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
                WHEN oo.ARDocType IN ( 'NA' ) THEN 0
                ELSE -1
            END ) * b.AfterVATAmount
    END
    ) AS AfterVATAmount,

    SUM(
    CASE
        WHEN b.FreeItem = 1 THEN 0
        ELSE
            (CASE
                WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
                WHEN oo.ARDocType IN ( 'NA' ) THEN 0
                ELSE -1
            END) * b.VATAmount
    END
    ) AS VATAmount,
    b.SlsPrice,
    BeforeVATPrice = ROUND(b.BeforeVATPrice, 0),
    o.OrderType,
    ReasonCode = o.ReasonCode,
    b.LineRef,
    b.SupID,
    b.ASM,
    b.RSM
    FROM dbo.OM_SalesOrd o WITH (NOLOCK)
    INNER JOIN OM_SalesOrdDet b WITH (NOLOCK) ON
    o.BranchID = b.BranchID AND
    o.OrderNbr = b.OrderNbr
    LEFT JOIN OM_LotTrans l WITH (NOLOCK) ON
    l.BranchID = b.BranchID AND
    l.OrderNbr = b.OrderNbr AND
    l.OMLineRef = b.LineRef
    LEFT JOIN #DataReturnIO oity ON
    oity.BranchID = o.BranchID AND
    oity.OrderNbr = o.OrderNbr
    INNER JOIN #TCpnyID r WITH (NOLOCK) ON
    r.CpnyID = o.BranchID
    INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK) ON
    oo.OrderType = o.OrderType AND
    ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
    INNER JOIN dbo.OM_SalesOrd a WITH (NOLOCK) ON
    o.BranchID = a.BranchID AND
    o.OrigOrderNbr = a.OrderNbr
    WHERE (o.Status = 'C')
    AND CAST(a.OrderDate AS DATE)
    BETWEEN @FromDate AND @ToDate
    AND o.SalesOrderType = 'RP' --  and o.invcnbr='0086713'
    AND oity.OrderNbr IS NULL
    GROUP BY ISNULL(l.LotSerNbr, ''),
    CAST(ISNULL(l.ExpDate, '') AS VARCHAR(20)),
    (o.OrdDiscAmt + o.VolDiscAmt),
    ROUND(b.BeforeVATPrice, 0),
    o.BranchID,
    a.OrderDate,
    o.CustID,
    a.OrigOrderNbr,
    o.OrderNbr,
    o.Crtd_User,
    o.Crtd_DateTime,
    o.ContractID,
    o.OrdAmt,
    o.InvcNbr,
    o.InvcNote,
    b.SlsperID,
    b.InvtID,
    b.FreeItem,
    a.DeliveryID,
    a.ShipDate,
    b.SlsPrice,
    o.OrderType,
    o.ReasonCode,
    b.LineRef,
    b.SupID,
    b.ASM,
    b.RSM
    ) so
LEFT JOIN dbo.OM_PDASalesOrd a WITH (NOLOCK) ON
so.BranchID = a.BranchID AND
so.OrigOrderNbr = a.OrderNbr
LEFT JOIN dbo.OM_SalesOrd a1 WITH (NOLOCK) ON
a.BranchID = a1.BranchID AND
a.OriOrderNbrUp = a1.OrderNbr
INNER JOIN #TOrderType r1 WITH (NOLOCK) ON
r1.OrderType = so.OrderType
LEFT JOIN OM_OriginalContract ctr WITH (NOLOCK) ON
so.ContractID = ctr.ContractID
GROUP BY
CASE
    WHEN so.OrigOrderNbr <> '' THEN so.OrigOrderNbr
    ELSE so.OrderNbr
END,
ISNULL(a1.OrigOrderNbr, ''),
ISNULL(a1.OrderDate, '19000101'),
CASE
    WHEN ISNULL(so.status, '') = '' THEN
        (CASE
            WHEN a.Status = 'C' THEN
            N'Đã Duyệt Đơn Hàng'
            WHEN a.Status = 'H' THEN
            N'Chờ Xử Lý'
            WHEN a.Status = 'E' THEN
            N'Đóng Đơn Hàng'
            WHEN a.Status = 'V' THEN
            N'Hủy Đơn Hàng'
        END)
    ELSE
        (CASE
            WHEN so.status = 'C' THEN
            N'Đã Phát Hành'
            WHEN so.status = 'I' THEN
            N'Tạo Hóa Đơn'
            WHEN so.status = 'N' THEN
            N'Tạo Hóa Đơn'
            WHEN so.status = 'H' THEN
            N'Chờ Xử Lý'
            WHEN so.status = 'E' THEN
            N'Đóng Đơn Hàng'
            WHEN so.status = 'V' THEN
            N'Hủy Hóa Đơn'
        END
)
END,
ISNULL(so.OrdAmt, 0),
ISNULL(so.InvcNbr, ''),
ISNULL(so.InvcNote, ''),
ISNULL(ctr.ContractNbr, ''),
CASE
    WHEN a.ReasonCode <> '' THEN a.ReasonCode
    ELSE so.ReasonCode
END,
so.BranchID,
so.OrderNbr,
so.SlsperID,
so.OrderDate,
so.CustID,
so.InvtID,
so.Lotsernbr,
so.ExpDate,
so.Crtd_User,
so.Crtd_DateTime,
so.ContractID,
so.DeliveryID,
so.ShipDate,
so.Qty,
so.LineRef,
so.OrderType,
so.SlsPrice,
so.BeforeVATPrice,
so.FreeItem,
so.SupID,
so.ASM,
so.RSM
) a

SELECT * FROM #Ord

-- ENDING LAY DS DON HANG
DROP TABLE IF EXISTS #Sales
SELECT DISTINCT
       a.BranchID,
       a.OrderNbr
INTO #Sales
FROM #Ord a
WHERE CAST(a.OrderDate AS DATE)
BETWEEN @FromDate AND @ToDate;


--GETTING DELIVERY DATA
DROP TABLE IF EXISTS #Deli
SELECT DISTINCT
a.BranchID,
a.BatNbr,
a.SlsperID,
a.Status,
a.OrderNbr,
ShipDate = ISNULL(c.ShipDate, a.Crtd_DateTime)
INTO #Deli
FROM dbo.OM_Delivery a WITH (NOLOCK)
INNER JOIN #Sales d ON 
d.BranchID = a.BranchID AND
d.OrderNbr = a.OrderNbr
INNER JOIN
(
SELECT
de.BranchID,
de.BatNbr,
de.OrderNbr,
Sequence = MAX(Sequence)
FROM dbo.OM_Delivery de
INNER JOIN #Sales d
ON d.BranchID = de.BranchID
AND d.OrderNbr = de.OrderNbr
GROUP BY 
de.BranchID,
de.BatNbr,
de.OrderNbr
) b ON 
b.BatNbr = a.BatNbr AND 
b.BranchID = a.BranchID AND 
b.Sequence = a.Sequence AND 
b.OrderNbr = a.OrderNbr
LEFT JOIN
(
SELECT de.BranchID,
de.BatNbr,
de.OrderNbr,
ShipDate = MAX(ShipDate)
FROM dbo.OM_DeliHistory de
INNER JOIN #Sales d ON
d.BranchID = de.BranchID AND
d.OrderNbr = de.OrderNbr
GROUP BY
de.BranchID,
de.BatNbr,
de.OrderNbr
) c ON
c.BatNbr = a.BatNbr AND
c.BranchID = a.BranchID AND
c.OrderNbr = a.OrderNbr;

SELECT * FROM #Deli