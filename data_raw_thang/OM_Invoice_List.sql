USE [PhaNam_eSales_PRO]
GO

/****** Object:  StoredProcedure [dbo].[pr_OM_InvoiceList]    Script Date: 30-12-2022 1:42:30 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



---- Select * from RPTRunning where reportname='OM_InvoiceList'  order by ReportID Desc
ALTER PROC [dbo].[pr_OM_InvoiceList] -- pr_OM_InvoiceList 209
@RPTID INT 
AS 
--DECLARE @RPTID INT =927
DECLARE @fromdate DATE
DECLARE @todate DATE 
DECLARE @UserName VARCHAR(30)
DECLARE @CpnyID VARCHAR(MAX)
DECLARE @LangID VARCHAR(2)
--DECLARE @RPTID INT

SELECT @fromdate = DateParm00 , @todate = DateParm01, @UserName=UserID,@LangID=LangID 
FROM dbo.RPTRunning 
WHERE ReportID =  @RPTID



SELECT K.BranchID, K.OrderNbr, K.Type, Rownumber=ROW_NUMBER()OVER (ORDER BY K.BranchID, K.OrderNbr ASC),
K.Patter, K.InvcNbr,K.InvcNote, K.InvoiceCustID,K.TaxRegNbr, K.OrderDate, K.CustInvcName,K.Status, K.DocType
INTO #TOrderList 
FROM ( 
SELECT so.BranchID, so.OrderNbr,so.Patter, so.InvcNbr, so.InvcNote,so.InvoiceCustID, so.TaxRegNbr, so.OrderDate, so.CustInvcName, Status= CASE WHEN  so1.InvcNbr IS not NULL THEN 'V' ELSE so.Status end ,  Type= 'OM' 
,DocType -- Ngochb bổ sung để kiểm tra loại chứng từ thể hiện tiền âm nếu hóa đơn chiết khấu
FROM dbo.OM_SalesOrd so WITH (NOLOCK)
INNER JOIN OM_Invoice iv  WITH (NOLOCK) on so.BranchID=iv.BranchID and so.ARRefNbr=iv.RefNbr and so.CustID=iv.CustID   
LEFT  JOIN OM_SalesOrd so1  WITH (NOLOCK) on so1.BranchID=iv.BranchID and so1.InvcNbr=iv.InvcNbr and so1.InvcNote=iv.InvcNote and so1.CustID=iv.CustID  AND so1.OrderType  IN ('CO','HK') and so1.Status= 'C'  -- Ngochb thêm trường hợp trả hàng hủy hóa đơn
INNER JOIN dbo.RPTRunningParm0 r WITH(NOLOCK) ON r.ReportID =  @RPTID AND r.StringParm = so.BranchID 
WHERE CAST(so.OrderDate AS DATE) BETWEEN @fromdate AND @todate 
and so.Status in ( 'C' , 'V') AND so.OrderType NOT IN ('CO','HK')   and  so.InvcNbr <>''
--UNION ALL  -- Ngochb thêm trường hợp trả hàng hủy hóa đơn
--SELECT so.BranchID, so.OrderNbr,so.Patter, so.InvcNbr, so.InvcNote,so.InvoiceCustID, so.TaxRegNbr, so.OrderDate, so.CustInvcName, Status='V',  Type= 'OM' 
--,DocType -- Ngochb bổ sung để kiểm tra loại chứng từ thể hiện tiền âm nếu hóa đơn chiết khấu
--FROM dbo.OM_SalesOrd so WITH (NOLOCK)
--INNER JOIN OM_Invoice iv  WITH (NOLOCK) on so.BranchID=iv.BranchID and so.ARRefNbr=iv.RefNbr and so.CustID=iv.CustID   
--INNER JOIN dbo.RPTRunningParm0 r WITH(NOLOCK) ON r.ReportID =  @RPTID AND r.StringParm = so.BranchID 
--WHERE CAST(so.OrderDate AS DATE) BETWEEN @fromdate AND @todate 
--and so.Status in ( 'C' , 'V') AND so.OrderType   IN ('CO') 
UNION ALL
SELECT so.BranchID, OrderNbr=cit.BatNbr,d.Patter, d.InvcNbr, d.InvcNote,InvoiceCustID=cit.CustIDInvoice, TaxRegNbr=cit.TaxID, OrderDate=d.DocDate,CustInvcName= cit.CustNameInvoice, 
Status= case when CancelBatNbrViettel =1 then 'V' else so.Status end, Type='AR' 
,DocType -- Ngochb bổ sung để kiểm tra loại chứng từ thể hiện tiền âm nếu hóa đơn chiết khấu
from Batch so  with(nolock)
INNER JOIN dbo.AR_Doc d WITH (NOLOCK) ON d.BatNbr = so.BatNbr AND d.BranchID = so.BranchID   
INNER JOIN dbo.AR_CustInvoiceTrans cit WITH(NOLOCK) ON cit.BatNbr = d.BatNbr AND cit.BranchID = d.BranchID 
INNER JOIN dbo.RPTRunningParm0 r WITH(NOLOCK) ON r.ReportID =  @RPTID AND r.StringParm = so.BranchID 
where    so.EditScrnNbr='AR10100'    and so.Module='AR'  and so.JrnlType='AR' AND so.Status in ( 'C' , 'V') 
AND so.ImpExp=1 and CAST(so.DateEnt AS DATE) BETWEEN @fromdate AND @todate   and  d.InvcNbr <>''
) K

CREATE TABLE #SalesDet (
		Line			INT,
		Code			VARCHAR(50),	
		KeyID			VARCHAR(50),	---- BranchID+OrderNbr
		LotNbr			VARCHAR(30),	---- Số Lô
		Extra			VARCHAR(10),	---- Hạn dùng
		ProdName		NVARCHAR(400),	---- Tên Sản Phẩm
		ProdUnit		NVARCHAR(15),	---- Đơn Vị Tính, lấy diễn giải
		ProdQuantity	FLOAT,			----- Số Lượng
		ProdPrice		FLOAT,			---- Giá trước thuế
		Total			FLOAT,			---- thành tiền trước thuế = ProdQuantity  * ProdPrice
		IsSum			VARCHAR(3),		----- 0: Hàng bán, 1: Hàng tặng, 2: Chiết khấu
		VATRate			FLOAT,			---- Thuế GTGT(%)(	-1: Không tính thuế, 
										----				0: Thuế = 0%, 
										----				10: thuế = 10%, 
										----				5: Thuế = 5%) 
										----				(-3 Đối với những dòng chỉ có tên sản phẩm)
		VATAmount		FLOAT,			---- Tiền thuế
		Amount			FLOAT,			---- Tổng tiền bao gồm thuế
		InvoiceDesc		NVARCHAR(MAX), 
		---- HAILH Modified On 17/11/2020: Chỉnh Sửa Hiển Thị Bỏ Số Thập Phân Phía Sau. 0,00001000000 => 0,00001
		DecimalPlaces	INT	DEFAULT(0),	---- Số Chữ Số Lẻ Sau Dấu Thập Phân của ProdPrice
		InvtID VARCHAR(20), ---mã sản phẩm mới bổ sung ngày 23/12/2021
)

DECLARE @MaxRownumber INT
DECLARE @intFlag INT
DECLARE @BranchID VARCHAR(30)
DECLARE @OrderNbr VARCHAR(30)
DECLARE @Type VARCHAR(2)
Set @MaxRownumber = 0 
Set @intFlag = 0 
SET @MaxRownumber = ( Select Max(Rownumber) as Rownumber FROM #TOrderList )
IF(@MaxRownumber>0)
		BEGIN	
	


				WHILE @intFlag <= @MaxRownumber
						BEGIN
						SELECT @BranchID=BranchID, @OrderNbr=OrderNbr ,@Type=Type
						FROM #TOrderList WHERE Rownumber=@intFlag
						
						IF (@Type='OM')
						BEGIN
						
							INSERT INTO #SalesDet
							(
							    Line,
							    Code,
							    KeyID,
							    LotNbr,
							    Extra,
							    ProdName,
							    ProdUnit,
							    ProdQuantity,
							    ProdPrice,
							    Total,
							    IsSum,
							    VATRate,
							    VATAmount,
							    Amount,
							    InvoiceDesc,
							    DecimalPlaces,
								InvtID
							)
							
						   EXEC [GETOM_SALESORDETTOINVOICE] @UserName, @CpnyID, @LangID, @BranchID, @OrderNbr, '', 1, '#SalesDet'
						end
						ELSE IF (@Type='AR')
						BEGIN
						    INSERT INTO #SalesDet
							(
							    Line,
							    Code,
							    KeyID,
							    LotNbr,
							    Extra,
							    ProdName,
							    ProdUnit,
							    ProdQuantity,
							    ProdPrice,
							    Total,
							    IsSum,
							    VATRate,
							    VATAmount,
							    Amount,
							    InvoiceDesc,
							    DecimalPlaces
							)
							
						   EXEC [GETAR_SALESORDETTOINVOICE] @UserName, @CpnyID, @LangID, @BranchID, @OrderNbr, '', 1, '#SalesDet'
						END



						Set @intFlag = @intFlag +1;
						 IF (@intFlag > @MaxRownumber)
							BREAK  
						ELSE
							CONTINUE
						END
			
		
				
END
--SELECT * FROM #SalesDet WHERE KeyID='MR0011HD062021-00034'
----UPDATE t1 SET t1.IsSum = CASE T1.IsSum	
----										WHEN 0 THEN 1
----										WHEN 1 THEN 0
----										WHEN 2 THEN 1 END
----FROM #SalesDet T1
UPDATE T1 SET t1.IsSum = CASE WHEN t1.IsSum =2 THEN 1
						 WHEN l.Type='AR' AND t1.IsSum=0 THEN 1
						 WHEN l.Type='AR' AND t1.IsSum=1 THEN 0 ELSE IsSum end  
FROM #SalesDet T1 
INNER JOIN #TOrderList l ON t1.KeyID=l.BranchID+l.OrderNbr

--SELECT * FROM #SalesDet WHERE KeyID='MR0011HD062021-00034'
SELECT	TaxID00=ISNULL(sod.VATRate,'') , 
		Descr =  CASE WHEN ISNULL(sod.VATRate,'') = '' THEN  N'Hàng hoá, dịch vụ không chịu thuế giá trị gia tăng (GTGT)' 
									WHEN sod.VATRate = 0 THEN  N'Hàng hoá, dịch vụ chịu thuế suất thuế GTGT 0%'
									WHEN sod.VATRate = 5 THEN  N'Hàng hoá, dịch vụ chịu thuế suất thuế GTGT 5%'
									WHEN sod.VATRate = 10 THEN  N'Hàng hoá, dịch vụ chịu thuế suất thuế GTGT 10%'
									END , 
		TaxRate= (Case when so.DocType in ('IN','DM','NA','CS') then 1 else -1 end) *ISNULL(sod.VATRate,'') ,
		--TaxRate= ISNULL(sod.VATRate,'') ,
		so.OrderNbr , StartDate = @fromdate , EndDate = @todate ,
		so.BranchID ,com.CpnyName, CustID = so.InvoiceCustID ,so.InvcNbr , so.Patter , so.InvcNote , so.OrderDate , CustName = so.CustInvcName, so.TaxRegNbr , 
		
		--TxblAmtTot00 = (Case when so.DocType in ('IN','DM','NA','CS') then 1 else -1 end) * (CASE WHEN so.Status='V' THEN 0 ELSE SUM(ISNULL(sod.IsSum*sod.Total,0)) end),  -- Ngochb bổ sung đk so.DocType
		--TaxAmtTot00 = (Case when so.DocType in ('IN','DM','NA','CS') then 1 else -1 end) * (CASE WHEN so.Status='V' THEN 0 ELSE SUM(ISNULL(sod.IsSum*Round(sod.VATAmount,0) ,0)) end), 
		--dattv update ngày 20220517 
		
		TxblAmtTot00 = (Case when so.DocType in ('IN','DM','NA','CS') then 1 else -1 end) * (CASE WHEN so.Status='V' THEN 0 ELSE SUM(ISNULL(sod.IsSum*sod.Total,0)) end),  -- Ngochb bổ sung đk so.DocType
		TaxAmtTot00 = (Case when so.DocType in ('IN','DM','NA','CS') then 1 else -1 end) * (CASE WHEN so.Status='V' THEN 0 ELSE SUM(ISNULL(sod.IsSum*Round(sod.VATAmount,0) ,0)) end), 
		Status  =  CASE WHEN so.Status = 'C' THEN N'Đã phát hành hóa đơn'
						WHEN so.Status = 'I' THEN N'Xử lý hoàn tất'
						WHEN so.Status = 'N' THEN N'Chờ xử lý'
						WHEN so.Status = 'V' THEN N'Đã hủy hóa đơn'

						END
					 ,
		[Kiểu Đơn Hàng] = ''

FROM dbo.#TOrderList so WITH(NOLOCK) 
INNER JOIN #SalesDet sod WITH(NOLOCK) ON sod.KeyID=so.BranchID+so.OrderNbr
LEFT JOIN dbo.SYS_Company com WITH(NOLOCK) ON com.CpnyID =  so.BranchID
WHERE ISNULL(sod.VATRate,'')<>-3
GROUP BY CASE
         WHEN sod.VATRate = '' THEN
         N'Hàng hoá, dịch vụ không chịu thuế giá trị gia tăng (GTGT)'
         WHEN sod.VATRate = 0 THEN
         N'Hàng hoá, dịch vụ chịu thuế suất thuế GTGT 0%'
         WHEN sod.VATRate = 5 THEN
         N'Hàng hoá, dịch vụ chịu thuế suất thuế GTGT 5%'
         WHEN sod.VATRate = 10 THEN
         N'Hàng hoá, dịch vụ chịu thuế suất thuế GTGT 10%'
         END,
         CASE
         WHEN so.Status = 'C' THEN
         N'Đã phát hành hóa đơn'
         WHEN so.Status = 'I' THEN
         N'Xử lý hoàn tất'
         WHEN so.Status = 'N' THEN
         N'Chờ xử lý'
         WHEN so.Status = 'V' THEN
         N'Đã hủy hóa đơn'
         --WHEN so.Status = 'C' AND coo.OrderNbr IS NOT NULL THEN N'Đã hủy hóa đơn'
         END,
         sod.VATRate,
         sod.VATRate,
         so.OrderNbr,
         so.BranchID,
         com.CpnyName,
         so.InvoiceCustID,
         so.InvcNbr,
         so.Patter,
         so.InvcNote,
         so.OrderDate,
         so.CustInvcName,
         so.TaxRegNbr,
         so.DocType,
         so.Status

Order by so.BranchID,so.OrderDate, so.InvcNote, so.InvcNbr



DROP TABLE #SalesDet
DROP TABLE #TOrderList


GO


