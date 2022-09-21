DECLARE @from DATE = '2022-04-01'
DECLARE @to DATE = '2022-05-09'
select
so.BranchID,
so.OrderNbr,
so.OrigOrderNbr,
so.CustID,
so.InvcNbr,
so.InvcNote,
so.Status as soStatus,
so.OrderType,
oo.ARDocType,
oo.Descr,
pso.Crtd_Prog,
so.PaymentsForm,
pso.BranchRouteID,
pso.SalesRouteID,
pso.InsertFrom,
so.Remark,
sod.LineQty,
sod.LineRef,
sod.SlsPerID,
sod.Invtid,
sod.FreeItem,
sod.BeforeVATPrice,
sod.AfterVATPrice,
sod.BeforeVATAmount,
sod.AfterVATAmount,
case when ib.DeliveryUnit = 'CW' then N'Chành Xe'
when ib.DeliveryUnit = 'PN' then N'Pha Nam' else 'NVC' end dvvc,
manvgh = '',
dStatus = ib.Status,

pso.Crtd_DateTime as post_time,
pso.Crtd_User as post_user,
ho.ErrorMessage as pending_reason,
pso.LUpd_DateTime as approve_time,
pso.LUpd_User as approve_user,
iv.LUpd_DateTime as invoice_time,
iv.LUpd_User as invoice_user,
ib.Crtd_DateTime as booked_time,
ib.Crtd_User as booked_user,
ready_to_ship_time = ib.LUpd_DateTime,
ib.Crtd_User as rts_user,
ib.LUpd_DateTime as delivered_time,
ib.Crtd_User as delivered_user,
datediff(minute, pso.Crtd_DateTime, pso.LUpd_DateTime) as leadtime_t0_minute,
datediff(minute, pso.LUpd_DateTime, iv.LUpd_DateTime) as leadtime_t1_minute,
datediff(minute, iv.LUpd_DateTime, ib.LUpd_DateTime) as leadtime_t2_minute,
datediff(minute, ib.LUpd_DateTime, ib.LUpd_DateTime) as leadtime_t3_minute,
datediff(minute, ib.LUpd_DateTime, ib.LUpd_DateTime) as leadtime_t4_minute,
datediff(minute, pso.Crtd_DateTime, ib.LUpd_DateTime) as leadtime_full_minute

from OM_SalesOrd so WITH(NOLOCK)
INNER JOIN OM_PDASalesOrd pso WITH(NOLOCK) ON
pso.BranchID = so.BranchID and
pso.OrderNbr = so.OrigOrderNbr
INNER JOIN dbo.OM_OrderType oo WITH(NOLOCK) ON oo.OrderType = so.OrderType
INNER JOIN dbo.OM_SalesOrdDet sod WITH(NOLOCK) ON 
so.BranchID = sod.BranchID AND sod.OrderNbr = so.OrderNbr
INNER JOIN OM_Issuebookdet ibd WITH(NOLOCK) ON
so.BranchID = ibd.BranchID and
so.OrigOrderNbr = ibd.OrderNbr
INNER JOIN OM_Issuebook ib WITH(NOLOCK) ON
ibd.BranchID = ib.BranchID and
ibd.BatNbr = ib.BatNbr and
Cast(ib.LUpd_DateTime as date) >= @from
and ib.Status != 'C'
LEFT JOIN OM_Invoice iv WITH(NOLOCK) ON
so.BranchID = iv.BranchID and
so.InvcNbr = iv.InvcNbr and
so.InvcNote = iv.InvcNote and
so.ARRefNbr = iv.RefNbr
LEFT JOIN API_HistoryOM205 ho WITH(NOLOCK) ON
so.BranchID = ho.BranchID and
so.OrigOrderNbr = ho.OrderNbr
and ho.Status = 'E'