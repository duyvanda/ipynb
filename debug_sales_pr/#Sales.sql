SELECT
BranchID,
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
    INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK) ON
    oo.OrderType = o.OrderType
    AND ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
    LEFT JOIN dbo.OM_PDASalesOrd a WITH (NOLOCK) ON
    o.BranchID = a.BranchID AND
    o.OrigOrderNbr = a.OrderNbr
    WHERE (o.Status = 'C')
    AND CAST(o.OrderDate AS DATE)
    BETWEEN '20210501' AND '20210531'
    AND o.SalesOrderType <> 'RP' --  and o.invcnbr='0086713'
    -- AND oity.OrderNbr IS NULL
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
    INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK) ON
    oo.OrderType = o.OrderType AND
    ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
    INNER JOIN dbo.OM_SalesOrd a WITH (NOLOCK) ON
    o.BranchID = a.BranchID AND
    o.OrigOrderNbr = a.OrderNbr
    WHERE (o.Status = 'C')
    AND CAST(a.OrderDate AS DATE)
    BETWEEN '20210501' AND '20210531'
    AND o.SalesOrderType = 'RP' --  and o.invcnbr='0086713'
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