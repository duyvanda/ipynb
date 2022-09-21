DROP TABLE IF EXISTS #Book
SELECT DISTINCT
ib.BranchID,
ib.SlsperID,
ib.BatNbr,
ibe.OrderNbr,
Name = FirstName,
trs.Descr,
DeliveryUnitName = ISNULL(d.DeliveryUnitName, ''),
TruckDescr = ISNULL(tr.Descr, '')
FROM OM_IssueBook ib WITH (NOLOCK)
LEFT JOIN OM_IssueBookDet ibe WITH (NOLOCK) ON
ibe.BranchID = ib.BranchID AND
ibe.BatNbr = ib.BatNbr
--INNER JOIN  OM_SalesOrd s  WITH(NOLOCK)  on s.BranchID = ibe.BranchID AND s.OrigOrderNbr = ibe.OrderNbr
INNER JOIN Users u WITH (NOLOCK) ON
u.UserName = ib.SlsperID
INNER JOIN AR_Transporter trs WITH (NOLOCK) ON
trs.Code = ib.DeliveryUnit
LEFT JOIN dbo.OM_ReceiptDet b WITH (NOLOCK) ON
b.BranchID = ibe.BranchID AND
b.OrderNbr = ibe.OrderNbr
LEFT JOIN dbo.OM_Receipt a WITH (NOLOCK) ON
b.ReportID = a.ReportID
LEFT JOIN OM_Truck tr WITH (NOLOCK) ON
a.TruckID = tr.Code AND
tr.BranchID = a.BranchID
LEFT JOIN dbo.OM_DeliReportDet de WITH (NOLOCK) ON
de.BranchID = ibe.BranchID AND
de.OrderNbr = ibe.OrderNbr
LEFT JOIN dbo.OM_DeliReport da WITH (NOLOCK) ON
da.ReportID = de.ReportID AND
de.BranchID = da.BranchID
LEFT JOIN dbo.AR_DeliveryUnit d WITH (NOLOCK) ON
da.DeliveryUnit = d.DeliveryUnitID AND
d.BranchID = da.BranchID;