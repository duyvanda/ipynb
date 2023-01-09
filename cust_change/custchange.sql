USE [PhaNam_eSales_PRO]
GO

/****** Object:  StoredProcedure [dbo].[pr_OM_RawDataChangeInfoCust]    Script Date: 05-01-2023 9:08:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--SELECT * FROM dbo.RPTRunning ORDER BY ReportID DESC
ALTER PROC [dbo].[pr_OM_RawDataChangeInfoCust] --  pr_OM_RawDataChangeInfoCust  1622      
    @RPTID INT
AS
DECLARE @ReportName NVARCHAR(100);
DECLARE @ReportNbr VARCHAR(10);
DECLARE @StartDate DATETIME;
DECLARE @EndDate DATETIME;
DECLARE @LogCom VARCHAR(10);
DECLARE @CpnyID VARCHAR(MAX);
DECLARE @TCpnyID TABLE
(
    CpnyID VARCHAR(MAX)
);
DECLARE @SlsPerID VARCHAR(MAX);
DECLARE @Status VARCHAR(50);
DECLARE @TSlsPerID TABLE
(
    SlsPerID VARCHAR(MAX)
);
DECLARE @Branch TABLE
(
    CpnyID VARCHAR(MAX)
);
DECLARE @UserID VARCHAR(50);

SELECT @ReportName = ReportName,
       @ReportNbr = ReportNbr,
       @StartDate = DateParm00,
       @EndDate = DateParm01,
       @LogCom = LoggedCpnyID,
       @SlsPerID = StringParm00,
       @Status = StringParm02,
       @UserID = UserID
FROM RPTRunning WITH (NOLOCK)
WHERE ReportID = @RPTID;


DECLARE @CompanyString VARCHAR(MAX);
SELECT @CompanyString = CpnyID
FROM vs_User WITH (NOLOCK)
WHERE UserName = @UserID;


IF NOT EXISTS
(
    SELECT ReportID
    FROM RPTRunningParm0 WITH (NOLOCK)
    WHERE ReportID = @RPTID
)
BEGIN
    INSERT INTO @TCpnyID
    SELECT DISTINCT
           c.CpnyID
    FROM vs_Company c WITH (NOLOCK)
    WHERE c.CpnyID IN (
                          SELECT Branch FROM fr_StrtoTable(@CompanyString)
                      );
END;
ELSE
BEGIN
    INSERT INTO @TCpnyID
    SELECT rp.StringParm
    FROM RPTRunningParm0 rp WITH (NOLOCK)
    WHERE rp.ReportID = @RPTID;
END;

--- Tạm comment cho đến khi HQ bổ sung field trong table log
--SELECT distinct a.CustID ,  b.Descr
--INTO #BusinessTemp
--FROM dbo.AR_HistoryCustClassID a WITH(NOLOCK)
--LEFT JOIN dbo.AR_BusinessScope b WITH(NOLOCK) ON (b.Code) IN (SELECT part FROM dbo.fr_SplitString(a.BusinessScope ,','))

--WHERE a.BusinessScope <> ''



--SELECT CustID ,  Descr  = STUFF(
--					 (SELECT ', ' + Descr 
--					  FROM #BusinessTemp t1
--					  WHERE t1.CustID = t2.CustID 
--					  FOR XML PATH (''))
--					 , 1, 1, '') 
--INTO #BusinessScope
--FROM #BusinessTemp t2
--group by CustID 


SELECT BranchID = c.BranchID,
       BranchName = co.CpnyName,
       CustID = c.CustID,
       MaKHCu = c.RefCustID,
       [KH Là Đại Lý Phân Phối] = CASE
                                      WHEN c.IsAgency = 1 THEN
                                          'X'
                                      ELSE
                                          ''
                                  END,
       [Khách Hàng Chung] = ge.CustName,
       CustName = c.CustName,
       [Mã Đại Lý Phân Phối] = c.AgencyID,
       [Tên Đại Lý Phân Phối] = ac.CustName,
       custaddress = ISNULL(c.Addr1 + ', ', '') --+ ISNULL(c.Addr2 + ', ', '')
                     + ISNULL(w.Name + ', ', '') + ISNULL(di.Name + ', ', '') + CASE c.State
                                                                                    WHEN '13' THEN
                                                                                        ''
                                                                                    WHEN '15' THEN
                                                                                        ''
                                                                                    WHEN '24' THEN
                                                                                        ''
                                                                                    WHEN '30' THEN
                                                                                        ''
                                                                                    WHEN '28' THEN
                                                                                        N'Thành Phố' + ' '
                                                                                    ELSE
                                                                                        N'Tỉnh' + ' '
                                                                                END + ISNULL(si.Descr, ''),
       [Khu Vực] = st1.Descr,
       [Mã Tỉnh/TP] = c.State,
       [Tỉnh/TP] = si.Descr,
       [Mã Quận/Huyện] = c.District,
       [Quận/Huyện] = di.Name,
       [Mã Phường/Xã] = c.Ward,
       [Phường/Xã] = w.Name,
       [Số Nhà Và Tên Đường] = c.Addr1,
       [Số Zip] = c.Zip,
       Tel = c.Phone,
       Fax = c.Fax,
       [Thư Điện Tử] = c.EMailAddr,
       [Mã HTBH] = c.SalesSystem,
       [Tên HTBH] = ss.Descr,
       ChannelID = c.Channel,
       ChannelName = ch.Descr,
       [Kênh Phụ] = c.ShopType,
       [Tên Kênh Phụ] = sty.Descr,
       HCOID = c.HCOID,
       HCOName = hco.HCOName,
       HCOTypeID = c.HCOType,
       [Tên Loại HCO] = hcoty.HCOTypeName,
       [Phân Hạng HCO] = c.ClassID,
       [Tên Phân Hạng HCO] = cl.Descr,
       SlsperID = ISNULL(c.SlsperId, ''),
       SlsName = s.Name,
       [Kiểm Tra Nợ] = CASE c.CheckTerm
                           WHEN N'N' THEN
                               N'Không Kiểm Tra'
                           WHEN N'D' THEN
                               N'Kiểm Tra Nợ Quá Hạn'
                           ELSE
                               ''
                       END,
       [Hạn Thanh Toán] = c.Terms,
       [Tên Hạn Thanh Toán] = Te.Descr,
       [HTT theo giá trị] = CASE
                                WHEN c.Terms = '12' THEN
                                    '120'
                                WHEN c.Terms = '18' THEN
                                    '180'
                                ELSE
                                    ISNULL(c.Terms, '')
                            END,
       Contact = ISNULL(c.Attn, ''),
       [Người Mua Hàng] = ISNULL(c.ShoperID, ''),

       --[Hình Thức Thanh Toán]=c.Paymentsform,
       [Tên Hình Thức Thanh Toán] = pa.Descr,
       --[Mã Tự Động Tạo HĐ]=c.GenOrders,
       [Tên Tự Động Tạo HĐ] = ord.Descr,
       --[Hình Thức Xuất Lô]=c.BatchExpForm,
       [Tên Hình Thức Xuất Lô] = ex.Descr,

       --[Phạm Vi Kinh Doanh] = bs.Descr,
       [Phương Pháp Kê Khai Thuế] = atx.Descr,
       [Doanh Số Khoán] = ass.Descr,
       [Mã Khách Hàng Thuế] = m.CustIDInvoice,
       [Mã Khách Hàng Thuế Cũ] = i.OldCustIDInvoice,
       [Tên Khách Hàng Thuế] = i.CustNameInvoice,
       [Mã Số Thuế] = i.TaxID,
       [Địa Chỉ Khách Hàng Thuế] = CONCAT(i.ApartNumber, ', ', iWard.Name, ', ', iDis.Name, ', ', iState.Descr),
       Status = CASE
                    WHEN c.Status = 'A' THEN
                        N'Đang Hoạt Động'
                    WHEN c.Status = 'I' THEN
                        N'Ngưng Hoạt Động'
                    WHEN c.Status = 'H' THEN
                        N'Chờ Xử Lý'
                END,
       [Người Tạo] = uct.FirstName,
       [Ngày Tạo] = CONVERT(VARCHAR (30), cust.Crtd_Datetime,114)+' '+ CONVERT(VARCHAR (30), cust.Crtd_Datetime,103),
       [Người Chỉnh Sửa] = us.FirstName,
       [Ngày Chỉnh Sửa] = CONVERT(VARCHAR (30), c.LUpd_Datetime,114)+' '+ CONVERT(VARCHAR (30), c.LUpd_Datetime,103)
FROM dbo.AR_HistoryCustClassID c WITH (NOLOCK)
INNER JOIN dbo.AR_Customer cust WITH (NOLOCK) ON c.CustID=cust.CustId
    LEFT JOIN dbo.AR_Customer_InvoiceCustomer m WITH (NOLOCK)
        ON m.CustID = c.CustID
           AND m.Active = 1
    LEFT JOIN dbo.AR_CustomerInvoice i WITH (NOLOCK)
        ON m.CustIDInvoice = i.CustIDInvoice
    LEFT JOIN dbo.SI_State iState WITH (NOLOCK)
        ON i.State = iState.State
    LEFT JOIN dbo.SI_District iDis WITH (NOLOCK)
        ON i.DistrictID = iDis.District
           AND iDis.State = i.State
    LEFT JOIN dbo.SI_Ward iWard WITH (NOLOCK)
        ON i.Ward = iWard.Ward
           AND i.DistrictID = iWard.District
           AND i.State = iWard.State
    LEFT JOIN dbo.AR_CustClass cl WITH (NOLOCK)
        ON cl.ClassId = c.ClassID
    LEFT JOIN AR_Salesperson s WITH (NOLOCK)
        ON s.BranchID = c.BranchID
           AND s.SlsperId = c.SlsperId
    LEFT JOIN vs_Company co WITH (NOLOCK)
        ON c.BranchID = co.CpnyID
    LEFT JOIN dbo.SI_Territory st WITH (NOLOCK)
        ON st.Territory = co.Territory
    LEFT JOIN dbo.SI_Territory st1 WITH (NOLOCK)
        ON st1.Territory = c.Territory
    LEFT JOIN dbo.SI_Zone sz WITH (NOLOCK)
        ON sz.Code = st.Zone
    LEFT JOIN dbo.AR_ShopType sty WITH (NOLOCK)
        ON sty.Code = c.ShopType
    LEFT JOIN dbo.AR_Channel ch WITH (NOLOCK)
        ON ch.Code = c.Channel
    LEFT JOIN dbo.SI_District di WITH (NOLOCK)
        ON c.District = di.District
           AND c.State = di.State
    LEFT JOIN dbo.SI_State si WITH (NOLOCK)
        ON si.State = c.State
    INNER JOIN SI_Country stc WITH (NOLOCK)
        ON stc.CountryID = si.Country
    LEFT JOIN dbo.SI_Ward w WITH (NOLOCK)
        ON w.Ward = c.Ward
           AND w.State = c.State
           AND w.District = c.District
    LEFT JOIN dbo.SYS_SalesSystem ss WITH (NOLOCK)
        ON c.SalesSystem = ss.Code
    LEFT JOIN dbo.AR_HCO hco WITH (NOLOCK)
        ON c.HCOID = hco.HCOID
    LEFT JOIN dbo.AR_HCOType hcoty WITH (NOLOCK)
        ON c.HCOType = hcoty.HCOTypeID
    LEFT JOIN dbo.SI_Terms Te WITH (NOLOCK)
        ON c.Terms = Te.TermsID
    LEFT JOIN dbo.AR_MasterPayments pa WITH (NOLOCK)
        ON c.PaymentsForm = pa.Code
    LEFT JOIN dbo.AR_MasterAutoGenOrder ord WITH (NOLOCK)
        ON c.GenOrders = ord.Code
    LEFT JOIN dbo.AR_PublicCust ge WITH (NOLOCK)
        ON c.CustIdPublic = ge.PubCust
    LEFT JOIN dbo.AR_MasterBatchExpForm ex WITH (NOLOCK)
        ON c.BatchExpForm = ex.Code
    LEFT JOIN Users us WITH (NOLOCK)
        ON UserName = c.LUpd_User
	LEFT JOIN dbo.Users uct WITH (NOLOCK) ON uct.UserName=cust.Crtd_User
    --LEFT JOIN #BusinessScope bs WITH (NOLOCK) ON bs.CustId = c.CustId -- Tạm comment chờ bổ sung field
    LEFT JOIN dbo.AR_TaxDeclaration atx WITH (NOLOCK)
        ON atx.Code = c.TaxDeclaration
    LEFT JOIN AR_StockSales ass WITH (NOLOCK)
        ON ass.Code = c.StockSales
    LEFT JOIN dbo.AR_Customer ac WITH (NOLOCK)
        ON ac.CustId = c.AgencyID
WHERE c.BranchID IN (
                        SELECT * FROM @TCpnyID
                    )
      AND CAST(c.LUpd_Datetime AS DATE)
      BETWEEN @StartDate AND @EndDate
ORDER BY c.CustID, c.LUpd_Datetime ASC

--DROP TABLE #BusinessScope


GO


