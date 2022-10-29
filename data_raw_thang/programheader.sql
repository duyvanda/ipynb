SELECT 
dc.Descr as GeneralPR, 
sq.DiscID,
sq.DiscSeq,
sq.Descr,
sq.TypeDiscount,
sq.DiscIDPN,
sq.ContentInvoice,
sq.AccumulateID,
sq.ExcludePromo,
sq.DiscFor,
sq.DiscClass,
db.BreakAmt,
db.BreakQty,
db.DiscAmt,
db.Descr as DetailDescr

FROM dbo.OM_DiscSeq sq
LEFT JOIN  dbo.OM_Discount dc ON
sq.DiscID = dc.DiscID
LEFT JOIN  dbo.OM_DiscBreak db ON
ds.DiscID = db.DiscID
and ds.DiscSeq = db.DiscSeq




--
select
--count(*)
dis.BranchID,
dis.OrderNbr,
sq.DiscID,
sq.DiscSeq,
sq.Descr,
sq.TypeDiscount,
sq.DiscIDPN,
sq.ContentInvoice,
sq.AccumulateID,
sq.ExcludePromo,
sq.DiscFor,
sq.DiscClass
from dbo.OM_OrdDisc dis
INNER JOIN dbo.OM_DiscSeq sq WITH (NOLOCK)
ON sq.DiscID = dis.DiscID
AND sq.DiscSeq = dis.DiscSeq
where cast (dis.Crtd_DateTime as date) >= '2022-10-01'


--LEFT JOIN #TOrdDisc dis WITH (NOLOCK)
--    ON dis.BranchID = a.BranchID
--       AND dis.OrderNbr = a.MaCT
--       AND dis.LineRef = a.LineRef