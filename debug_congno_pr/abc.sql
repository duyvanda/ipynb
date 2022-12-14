USE [PhaNam_eSales_PRO]
GO

/****** Object:  StoredProcedure [dbo].[pr_OM_RawdataSellOutPayroll_BI_v1]    Script Date: 28/03/2022 1:41:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





----  Select * from RPTRunning where ReportNbr='OLAP106' order by ReportID Desc

----  Select * from RPTRunning where ReportNbr='OLAP106' order by ReportID Desc

ALTER PROC [dbo].[pr_OM_RawdataSellOutPayroll_BI_v1] -- pr_OM_RawdataSellOutPayroll_BI_v1  '20211229','20211229'
    @FromDate DATE,
    @ToDate DATE
AS

--DECLARE  @FromDate DATE, @ToDate DATE
--SELECT @FromDate='20210805', @ToDate='20210805'
SET NOCOUNT ON
DECLARE @UserID VARCHAR(30) = 'Admin';
DECLARE @Terr VARCHAR(MAX) = '';
DECLARE @Zone VARCHAR(MAX) = '';
DECLARE @CurrentDate DATE = CAST(GETDATE() AS DATE);
-- Insert All Branch
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

-- Insert All OrderType

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


SELECT BranchID,
       SlsperID
INTO #SalesForce
FROM dbo.fr_ListSaleByData(@UserID);

-- AnhTT Lo???i c??c ????n tr??? h??ng CO, EP ko l???y l??n b??o c??o sellout
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
      BETWEEN @FromDate AND @ToDate
      AND o.OrderDate >= '20210501'
      AND o.OrderType IN ( 'CO', 'EP' )
      AND o.Status = 'C';




SELECT *
INTO #Customer
FROM dbo.vs_AR_CustomerInfo cu WITH (NOLOCK);
--WHERE (cu.Territory LIKE CASE WHEN @Terr = '' THEN '%' END OR cu.Territory IN (SELECT part FROM dbo.fr_SplitStringMAX(@Terr,',')))
--AND (cu.State LIKE CASE WHEN @Zone = '' THEN '%' END OR cu.State IN (SELECT part FROM dbo.fr_SplitStringMAX(@Zone,',')))

--SELECT DENSE_RANK() OVER (ORDER BY T1.DocDate, T2.OrigOrderNbr) AS OrderNo,
       ---- HAILH Modified On 22/07/2020: B??? Sung Th??ng Tin BatNbr, RefNbr ????? truy???n gi?? tr??? xu???ng PDA
 SELECT      T1.BranchID,
       T1.BatNbr,
       T1.RefNbr,
       T1.CustId,
       T2.OrigOrderNbr AS OrderNbr,
       T1.InvcNbr,
       T1.InvcNote,
       T1.DocDate AS InvoiceDate,
       T1.OrigDocAmt AS InvoiceAmount,
       ---- HAILH Modified On 16/07/2020: B??? sung x??t th???i h???n thanh to??n theo H???p ?????ng n???u c??
       COALESCE(T5.DueType, T3.DueType, '') AS DueType,
       COALESCE(T5.DueIntrv, T3.DueIntrv, '') AS DueIntrv,
       T1.DueDate,
       T3.Descr AS PaymentTerm,
       T1.OrigDocAmt - T1.DocBal AS PaidAmount,
       T1.DocBal AS RemainAmount,
       --'' AS DebtStatus,
       --'' AS Color ,
       OverPaymentTerm = IIF(T1.DueDate >= GETDATE(), 0, DATEDIFF(DAY, T1.DueDate, GETDATE()))
INTO #Doc
FROM AR_Doc T1 WITH (NOLOCK)
    INNER JOIN OM_SalesOrd T2 WITH (NOLOCK)
        ON T1.BranchID = T2.BranchID
           AND T1.RefNbr = T2.ARRefNbr
           AND T1.BatNbr = T2.ARBatNbr
    INNER JOIN #TCpnyID r WITH (NOLOCK)
        ON r.CpnyID = T1.BranchID
    INNER JOIN SI_Terms T3 WITH (NOLOCK)
        ON T2.Terms = T3.TermsID
    ---- HAILH Modified On 16/07/2020: B??? sung x??t th???i h???n thanh to??n theo H???p ?????ng n???u c??
    LEFT JOIN OM_OriginalContract T4 WITH (NOLOCK)
        ON T2.ContractID = T4.ContractID
    LEFT JOIN SI_Terms T5 WITH (NOLOCK)
        ON T4.Terms = T5.TermsID
WHERE CAST(T2.OrderDate AS DATE)
BETWEEN @FromDate AND @ToDate; --and T1.invcnbr='0086899'


--SELECT a.BranchID,
--       a.OrderNo,
--       a.CustId,
--       a.BatNbr,
--       RefNbr,
--       DebtStatus = T3.DebtStatusDescr,
--       Color = T3.DebtStatusColor
--INTO #DebtStatus
--FROM #Doc a
--    LEFT JOIN SI_DebtStatusSetup T2 WITH (NOLOCK)
--        ON a.DueType = T2.DueType
--    LEFT JOIN SI_DebtStatus T3 WITH (NOLOCK)
--        ON T2.DebtStatusCode = T3.DebtStatusCode
--WHERE a.OverPaymentTerm
--      BETWEEN T2.DOverFrom AND T2.DOverTo
--      AND a.OverPaymentTerm
--      BETWEEN (ROUND(T2.TOverDaysFrom * a.DueIntrv, 0) + T2.AddDaysFrom) AND ROUND(T2.TOverDaysTo * a.DueIntrv, 0)
--      AND PaidAmount <> 0;


SELECT BranchID,
       OrderNbr,
       MaCT,
       SlsperID,
       OrderDate,
       ReturnOrder,
       ReturnOrderdate,
       InvtID,
	   siteID,
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
	   PaymentsForm, -- ADD NEW 28032022
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
           OrderNbr = CASE
                          WHEN so.OrigOrderNbr <> '' THEN
                              so.OrigOrderNbr
                          ELSE
                              so.OrderNbr
                      END,
           MaCT = so.OrderNbr,
           so.SlsperID,
           so.OrderDate,
           ReturnOrder = ISNULL(a1.OrigOrderNbr, ''),
           ReturnOrderdate = ISNULL(a1.OrderDate, '19000101'),
           Status = CASE
                        WHEN ISNULL(so.status, '') = '' THEN
           (CASE
                WHEN a.Status = 'C' THEN
                    N'???? Duy???t ????n H??ng'
                WHEN a.Status = 'H' THEN
                    N'Ch??? X??? L??'
                WHEN a.Status = 'E' THEN
                    N'????ng ????n H??ng'
                WHEN a.Status = 'V' THEN
                    N'H???y ????n H??ng'
            END
           )
                        ELSE
           (CASE
                WHEN so.status = 'C' THEN
                    N'???? Ph??t H??nh'
                WHEN so.status = 'I' THEN
                    N'T???o H??a ????n'
                WHEN so.status = 'N' THEN
                    N'T???o H??a ????n'
                WHEN so.status = 'H' THEN
                    N'Ch??? X??? L??'
                WHEN so.status = 'E' THEN
                    N'????ng ????n H??ng'
                WHEN so.status = 'V' THEN
                    N'H???y H??a ????n'
            END
           )
                    END,
           so.CustID,
           so.InvtID,
		   so.SiteID,
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
		   so.PaymentsForm, -- ADD NEW 28032022
           so.LineRef,
           ChietKhau = SUM(so.ChietKhau),
           so.OrderType,
           ContractNbr = ISNULL(ctr.ContractNbr, ''),
           so.SlsPrice,
           so.BeforeVATPrice,
           so.FreeItem,
           ReasonCode = CASE
                            WHEN a.ReasonCode <> '' THEN
                                a.ReasonCode
                            ELSE
                                so.ReasonCode
                        END,
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
			   o.PaymentsForm, -- ADD NEW 28032022
               b.SlsperID,
               b.InvtID,
			   b.SiteID,
               b.FreeItem,
               Qty = SUM(ISNULL(l.Qty, b.LineQty)),
               Lotsernbr = ISNULL(l.LotSerNbr, ''),
               a.DeliveryID,
               a.ShipDate,
               ExpDate = CAST(ISNULL(l.ExpDate, '') AS VARCHAR(20)),
               ChietKhau = (o.OrdDiscAmt + o.VolDiscAmt),
               BeforeVATAmount = SUM(   CASE
                                            WHEN b.FreeItem = 1 THEN
                                                0
                                            ELSE
                                        (CASE
                                             WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                                                 1
                                             WHEN oo.ARDocType IN ( 'NA' ) THEN
                                                 0
                                             ELSE
                                                 -1
                                         END
                                        ) * b.BeforeVATAmount
                                        END
                                    ),
               AfterVATAmount = SUM(   CASE
                                           WHEN b.FreeItem = 1 THEN
                                               0
                                           ELSE
                                       (CASE
                                            WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                                                1
                                            WHEN oo.ARDocType IN ( 'NA' ) THEN
                                                0
                                            ELSE
                                                -1
                                        END
                                       ) * b.AfterVATAmount
                                       END
                                   ),
               VATAmount = SUM(   CASE
                                      WHEN b.FreeItem = 1 THEN
                                          0
                                      ELSE
                                  (CASE
                                       WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                                           1
                                       WHEN oo.ARDocType IN ( 'NA' ) THEN
                                           0
                                       ELSE
                                           -1
                                   END
                                  ) * b.VATAmount
                                  END
                              ),
               b.SlsPrice,
               BeforeVATPrice = ROUND(b.BeforeVATPrice, 0),
               o.OrderType,
               ReasonCode = o.ReasonCode,
               b.LineRef,
			   b.SupID,
			   b.ASM,
			   b.RSM
        FROM dbo.OM_SalesOrd o WITH (NOLOCK)
            INNER JOIN OM_SalesOrdDet b WITH (NOLOCK)
                ON o.BranchID = b.BranchID
                   AND o.OrderNbr = b.OrderNbr
            LEFT JOIN OM_LotTrans l WITH (NOLOCK)
                ON l.BranchID = b.BranchID
                   AND l.OrderNbr = b.OrderNbr
                   AND l.OMLineRef = b.LineRef
            LEFT JOIN #DataReturnIO oity
                ON oity.BranchID = o.BranchID
                   AND oity.OrderNbr = o.OrderNbr
            INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK)
                ON oo.OrderType = o.OrderType
                   AND ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
            INNER JOIN #TOrderType r1 WITH (NOLOCK)
                ON r1.OrderType = oo.OrderType
            INNER JOIN #TCpnyID r WITH (NOLOCK)
                ON r.CpnyID = o.BranchID
            LEFT JOIN dbo.OM_PDASalesOrd a WITH (NOLOCK)
                ON o.BranchID = a.BranchID
                   AND o.OrigOrderNbr = a.OrderNbr
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
				 o.PaymentsForm, -- ADD NEW 28032022
                 b.SlsperID,
                 b.InvtID,
				 b.SiteID,
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
			   o.PaymentsForm, -- ADD NEW 28032022
               b.SlsperID,
               b.InvtID,
			   b.SiteID,
               b.FreeItem,
               Qty = SUM(ISNULL(l.Qty, b.LineQty)),
               Lotsernbr = ISNULL(l.LotSerNbr, ''),
               a.DeliveryID,
               a.ShipDate,
               ExpDate = CAST(ISNULL(l.ExpDate, '') AS VARCHAR(20)),
               ChietKhau = (o.OrdDiscAmt + o.VolDiscAmt),
               BeforeVATAmount = SUM(   CASE
                                            WHEN b.FreeItem = 1 THEN
                                                0
                                            ELSE
                                        (CASE
                                             WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                                                 1
                                             WHEN oo.ARDocType IN ( 'NA' ) THEN
                                                 0
                                             ELSE
                                                 -1
                                         END
                                        ) * b.BeforeVATAmount
                                        END
                                    ),
               AfterVATAmount = SUM(   CASE
                                           WHEN b.FreeItem = 1 THEN
                                               0
                                           ELSE
                                       (CASE
                                            WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                                                1
                                            WHEN oo.ARDocType IN ( 'NA' ) THEN
                                                0
                                            ELSE
                                                -1
                                        END
                                       ) * b.AfterVATAmount
                                       END
                                   ),
               VATAmount = SUM(   CASE
                                      WHEN b.FreeItem = 1 THEN
                                          0
                                      ELSE
                                  (CASE
                                       WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                                           1
                                       WHEN oo.ARDocType IN ( 'NA' ) THEN
                                           0
                                       ELSE
                                           -1
                                   END
                                  ) * b.VATAmount
                                  END
                              ),
               b.SlsPrice,
               BeforeVATPrice = ROUND(b.BeforeVATPrice, 0),
               o.OrderType,
               ReasonCode = o.ReasonCode,
               b.LineRef,
			   b.SupID,
			   b.ASM,
			   b.RSM
        FROM dbo.OM_SalesOrd o WITH (NOLOCK)
            INNER JOIN OM_SalesOrdDet b WITH (NOLOCK)
                ON o.BranchID = b.BranchID
                   AND o.OrderNbr = b.OrderNbr
            LEFT JOIN OM_LotTrans l WITH (NOLOCK)
                ON l.BranchID = b.BranchID
                   AND l.OrderNbr = b.OrderNbr
                   AND l.OMLineRef = b.LineRef
            LEFT JOIN #DataReturnIO oity
                ON oity.BranchID = o.BranchID
                   AND oity.OrderNbr = o.OrderNbr
            INNER JOIN #TCpnyID r WITH (NOLOCK)
                ON r.CpnyID = o.BranchID
            INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK)
                ON oo.OrderType = o.OrderType
                   AND ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
            INNER JOIN dbo.OM_SalesOrd a WITH (NOLOCK)
                ON o.BranchID = a.BranchID
                   AND o.OrigOrderNbr = a.OrderNbr
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
				 o.PaymentsForm, -- ADD NEW 28032022
                 b.SlsperID,
                 b.InvtID,
				 b.SiteID,
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
        LEFT JOIN dbo.OM_PDASalesOrd a WITH (NOLOCK)
            ON so.BranchID = a.BranchID
               AND so.OrigOrderNbr = a.OrderNbr
        LEFT JOIN dbo.OM_SalesOrd a1 WITH (NOLOCK)
            ON a.BranchID = a1.BranchID
               AND a.OriOrderNbrUp = a1.OrderNbr
        --INNER JOIN(Select * from dbo.OM_PDASalesOrdDet   WITH(NOLOCK))b ON b.BranchID = a.BranchID AND b.OrderNbr = a.OrderNbr
        INNER JOIN #TOrderType r1 WITH (NOLOCK)
            ON r1.OrderType = so.OrderType
        --LEFT JOIN dbo.API_PostHistory p WITH(NOLOCK) ON a.BranchID = p.DmsBranchID and a.OrderNbr=p.DmsOrderNbr

        LEFT JOIN OM_OriginalContract ctr WITH (NOLOCK)
            ON so.ContractID = ctr.ContractID
    --WHERE CAST(a.OrderDate AS DATE) BETWEEN @fromdate AND @todate and a.Status='C'
    GROUP BY CASE
             WHEN so.OrigOrderNbr <> '' THEN
             so.OrigOrderNbr
             ELSE
             so.OrderNbr
             END,
             ISNULL(a1.OrigOrderNbr, ''),
             ISNULL(a1.OrderDate, '19000101'),
             CASE
             WHEN ISNULL(so.status, '') = '' THEN
             (CASE
             WHEN a.Status = 'C' THEN
             N'???? Duy???t ????n H??ng'
             WHEN a.Status = 'H' THEN
             N'Ch??? X??? L??'
             WHEN a.Status = 'E' THEN
             N'????ng ????n H??ng'
             WHEN a.Status = 'V' THEN
             N'H???y ????n H??ng'
             END
             )
             ELSE
             (CASE
             WHEN so.status = 'C' THEN
             N'???? Ph??t H??nh'
             WHEN so.status = 'I' THEN
             N'T???o H??a ????n'
             WHEN so.status = 'N' THEN
             N'T???o H??a ????n'
             WHEN so.status = 'H' THEN
             N'Ch??? X??? L??'
             WHEN so.status = 'E' THEN
             N'????ng ????n H??ng'
             WHEN so.status = 'V' THEN
             N'H???y H??a ????n'
             END
             )
             END,
             ISNULL(so.OrdAmt, 0),
             ISNULL(so.InvcNbr, ''),
             ISNULL(so.InvcNote, ''),
             ISNULL(ctr.ContractNbr, ''),
             CASE
             WHEN a.ReasonCode <> '' THEN
             a.ReasonCode
             ELSE
             so.ReasonCode
             END,
             so.BranchID,
             so.OrderNbr,
             so.SlsperID,
             so.OrderDate,
             so.CustID,
             so.InvtID,
			 so.SiteID,
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

 SELECT DISTINCT
        a.BranchID,
        a.OrderNbr
 INTO #Sales
 FROM #Ord a
 WHERE CAST(a.OrderDate AS DATE)
 BETWEEN @FromDate AND @ToDate;



-- SELECT DISTINCT
--        ord.BranchID,
--        ord.OrderNbr,
--        d.InvtID,
--        d.LineRef,
--        dis.FreeItemID,
--        sq.TypeDiscount,
--        DiscAmt = CASE
--                      WHEN dis.DiscType = 'L' THEN
--                          d.DiscAmt
--                      WHEN dis.DiscType = 'G' THEN
--                          d.GroupDiscAmt1
--                      WHEN dis.DiscType = 'D' THEN
--                          d.DocDiscAmt
--                  END,
--        DiscPct = CASE
--                      WHEN dis.DiscType = 'L' THEN
--                          d.DiscPct
--                      WHEN dis.DiscType = 'G' THEN
--                          d.GroupDiscPct1
--                      WHEN dis.DiscType = 'D' THEN
--                          d.DocDiscAmt -- Ch??a bi???t t??nh nh?? th??? n??o
--                  END,
--        sq.DiscIDPN,
--        sq.DiscID,
--        sq.DiscSeq,
--        dis.SOLineRef,
--        sq.Descr
-- INTO #TOrdDisc1
-- FROM dbo.OM_SalesOrd ord WITH (NOLOCK)
--     INNER JOIN dbo.OM_SalesOrdDet d WITH (NOLOCK)
--         ON d.BranchID = ord.BranchID
--            AND d.OrderNbr = ord.OrderNbr
--     INNER JOIN dbo.OM_OrdDisc dis WITH (NOLOCK)
--         ON dis.BranchID = d.BranchID
--            AND dis.OrderNbr = d.OrderNbr
--            AND d.LineRef IN (
--                                 SELECT part FROM dbo.fr_SplitStringMAX(dis.GroupRefLineRef, ',')
--                             )
--     INNER JOIN dbo.OM_DiscSeq sq WITH (NOLOCK)
--         ON sq.DiscID = dis.DiscID
--            AND sq.DiscSeq = dis.DiscSeq
--     INNER JOIN dbo.#Sales s WITH (NOLOCK)
--         ON ord.BranchID = s.BranchID
--            AND ord.OrigOrderNbr = s.OrderNbr
-- WHERE CAST(ord.OrderDate AS DATE)
-- BETWEEN @FromDate AND @ToDate; --   and ord.invcnbr='0086713'



-- SELECT DISTINCT
--        d.BranchID,
--        d.OrderNbr,
--        d.InvtID,
--        d.LineRef,
--        d.TypeDiscount,
--        d.DiscAmt,
--        d.DiscPct,
--        d.DiscIDPN,
--        d.DiscID,
--        d.DiscSeq,
--        d.Descr
-- INTO #TOrdDisc
-- FROM #TOrdDisc1 d
-- WHERE d.FreeItemID = '';

-- --- L???y danh s??ch s???n ph???m khuy???n m??i
-- CREATE TABLE #TDiscFreeItem
-- (
--     BranchID VARCHAR(30),
--     OrderNbr VARCHAR(30),
--     FreeItemID VARCHAR(30),
--     TypeDiscount VARCHAR(30),
--     DiscAmt FLOAT,
--     DiscPct FLOAT,
--     DiscIDPN VARCHAR(30),
--     DiscID VARCHAR(30),
--     DiscSeq VARCHAR(30),
--     SOLineRef VARCHAR(30),
--     Descr NVARCHAR(MAX)
-- );
-- INSERT INTO #TDiscFreeItem
-- (
--     BranchID,
--     OrderNbr,
--     FreeItemID,
--     TypeDiscount,
--     DiscAmt,
--     DiscPct,
--     DiscIDPN,
--     DiscID,
--     DiscSeq,
--     SOLineRef,
--     Descr
-- )
-- SELECT DISTINCT
--        dis.BranchID,
--        dis.OrderNbr,
--        dis.FreeItemID,
--        dis.TypeDiscount,
--        0,
--        0,
--        dis.DiscIDPN,
--        dis.DiscID,
--        dis.DiscSeq,
--        dis.SOLineRef,
--        dis.Descr
-- FROM #TOrdDisc1 dis
--     INNER JOIN dbo.OM_SalesOrdDet d
--         ON dis.BranchID = d.BranchID
--            AND dis.OrderNbr = d.OrderNbr
--            AND dis.FreeItemID = d.InvtID
--            AND dis.SOLineRef = d.LineRef
-- WHERE FreeItemID <> ''
--       AND d.FreeItem = 1;

-- INSERT INTO #TDiscFreeItem
-- (
--     BranchID,
--     OrderNbr,
--     FreeItemID,
--     TypeDiscount,
--     DiscAmt,
--     DiscPct,
--     DiscIDPN,
--     DiscID,
--     DiscSeq,
--     SOLineRef,
--     Descr
-- )
-- SELECT DISTINCT
--        ord.BranchID,
--        ord.OrderNbr,
--        pdis.FreeItemID,
--        sq.TypeDiscount,
--        DiscAmt = 0,
--        DiscPct = 0,
--        sq.DiscIDPN,
--        sq.DiscID,
--        sq.DiscSeq,
--        SOLineRef = d.LineRef,
--        sq.Descr
-- FROM #Sales bat
--     INNER JOIN dbo.OM_SalesOrd ord WITH (NOLOCK)
--         ON ord.BranchID = bat.BranchID
--            AND bat.OrderNbr = ord.OrigOrderNbr
--     INNER JOIN dbo.OM_SalesOrdDet d WITH (NOLOCK)
--         ON d.BranchID = ord.BranchID
--            AND d.OrderNbr = ord.OrderNbr
--     INNER JOIN #TCpnyID r WITH (NOLOCK)
--         ON r.CpnyID = ord.BranchID
--     INNER JOIN dbo.OM_PDAOrdDisc pdis WITH (NOLOCK)
--         ON pdis.BranchID = d.BranchID
--            AND pdis.OrderNbr = d.OrigOrderNbr
--            AND d.InvtID = pdis.FreeItemID
--            AND d.FreeItem = 1
--            AND d.OriginalLineRef = pdis.SOLineRef
--     INNER JOIN dbo.OM_DiscSeq sq WITH (NOLOCK)
--         ON sq.DiscID = pdis.DiscID
--            AND sq.DiscSeq = pdis.DiscSeq
--     LEFT JOIN #TDiscFreeItem dis WITH (NOLOCK)
--         ON dis.BranchID = d.BranchID
--            AND dis.FreeItemID = d.InvtID
--            AND d.OrderNbr = dis.OrderNbr
--            AND d.FreeItem = 1
--            AND dis.SOLineRef = d.LineRef
-- WHERE dis.OrderNbr IS NULL;

SELECT DISTINCT
       a.BranchID,
       a.BatNbr,
       a.SlsperID,
       a.Status,
       a.OrderNbr,
       ShipDate = a.LUpd_DateTime
INTO #Deli
FROM dbo.OM_Delivery a WITH (NOLOCK)
    INNER JOIN #Sales d
        ON d.BranchID = a.BranchID
           AND d.OrderNbr = a.OrderNbr
    INNER JOIN
    (
        SELECT de.BranchID,
               de.BatNbr,
               de.OrderNbr,
               Sequence = MAX(Sequence)
        FROM dbo.OM_Delivery de
		INNER JOIN #Sales d
        ON d.BranchID = de.BranchID
           AND d.OrderNbr = de.OrderNbr
        GROUP BY de.BranchID,
                 de.BatNbr,
                 de.OrderNbr
    ) b
        ON b.BatNbr = a.BatNbr
           AND b.BranchID = a.BranchID
           AND b.Sequence = a.Sequence
           AND b.OrderNbr = a.OrderNbr
    LEFT JOIN
    (
        SELECT de.BranchID,
               de.BatNbr,
               de.OrderNbr,
               ShipDate = MAX(ShipDate)
        FROM dbo.OM_DeliHistory de
		INNER JOIN #Sales d
        ON d.BranchID = de.BranchID
           AND d.OrderNbr = de.OrderNbr
        GROUP BY de.BranchID,
                 de.BatNbr,
                 de.OrderNbr
    ) c
        ON c.BatNbr = a.BatNbr
           AND c.BranchID = a.BranchID
           AND c.OrderNbr = a.OrderNbr;

SELECT DISTINCT
       ib.BranchID,
       ib.SlsperID,
       ib.BatNbr,
       ibe.OrderNbr,
       Name = FirstName,
       trs.Descr,
       DeliveryUnitName = ISNULL(d.DeliveryUnitName, ''),
       TruckDescr = ISNULL(tr.Descr, '')
INTO #Book
FROM OM_IssueBook ib WITH (NOLOCK)
    INNER JOIN OM_IssueBookDet ibe WITH (NOLOCK)
        ON ibe.BranchID = ib.BranchID
           AND ibe.BatNbr = ib.BatNbr
	INNER JOIN #Sales dd WITH (NOLOCK) ON   dd.BranchID = ibe.BranchID
           AND dd.OrderNbr = ibe.OrderNbr
    --INNER JOIN  OM_SalesOrd s  WITH(NOLOCK)  on s.BranchID = ibe.BranchID AND s.OrigOrderNbr = ibe.OrderNbr
    INNER JOIN Users u WITH (NOLOCK)
        ON u.UserName = ib.SlsperID
    INNER JOIN AR_Transporter trs WITH (NOLOCK)
        ON trs.Code = ib.DeliveryUnit
    LEFT JOIN dbo.OM_ReceiptDet b WITH (NOLOCK)
        ON b.BranchID = ibe.BranchID
           AND b.OrderNbr = ibe.OrderNbr
    LEFT JOIN dbo.OM_Receipt a WITH (NOLOCK)
        ON b.ReportID = a.ReportID
    LEFT JOIN OM_Truck tr WITH (NOLOCK)
        ON a.TruckID = tr.Code
           AND tr.BranchID = a.BranchID
    LEFT JOIN dbo.OM_DeliReportDet de WITH (NOLOCK)
        ON de.BranchID = ibe.BranchID
           AND de.OrderNbr = ibe.OrderNbr
    LEFT JOIN dbo.OM_DeliReport da WITH (NOLOCK)
        ON da.ReportID = de.ReportID
           AND de.BranchID = da.BranchID
    LEFT JOIN dbo.AR_DeliveryUnit d WITH (NOLOCK)
        ON da.DeliveryUnit = d.DeliveryUnitID
           AND d.BranchID = da.BranchID;

--select * from #Ord
--select * from #TOrdDisc
--select * from #TDiscFreeItem

SELECT DISTINCT [M?? C??ng Ty/CN] = ISNULL(a.BranchID, ''),
       [C??ng Ty/CN] = ISNULL(com.CpnyName, ''),
       --[?????a Ch??? C??ng Ty/CN] = ISNULL(com.Address,'') ,

       [Ng??y Ch???ng T???] = ISNULL(a.OrderDate, ''),
       [S??? ????n ?????t H??ng] = ISNULL(a.OrderNbr, ''),
       [MaHD] = a.MaCT, -- #ADDNEW
       [S??? ????n Tr??? H??ng] = ISNULL(a.ReturnOrder, ''),
       [Ng??y Tr??? H??ng] = ISNULL(CONVERT(VARCHAR(10), a.ReturnOrderdate, 103), ''),
       [H??a ????n] = ISNULL(a.InvcNbr, ''),
    --    [Ng??y T???i H???n TT] = ISNULL(CONVERT(VARCHAR(10), b.DueDate, 103), ''),
    --    [S??? H???p ?????ng] = ISNULL(con.ContractNbr, ''),
       [Tr???ng Th??i] = a.Status,
       --[M?? KH Thu???] = ISNULL(cu.CustIDInvoice, ''),
    --    [T??n KH Thu???] = ISNULL(cu.CustNameInvoice, ''),
    --    [?????a Ch??? KH Thu???] = ISNULL(cu.CustInvoiceAddr, ''),
    --    [M?? S??? Thu???] = ISNULL(cu.TaxID, ''),
        [M?? KH DMS] = ISNULL(a.CustID, ''),
       [M?? KH C??] = ISNULL(cu.RefCustID, ''),
       [T??n Kh??ch H??ng] = ISNULL(cu.CustName, ''),
	   [THTT] = b.PaymentTerm,
	   [PMT] = a.PaymentsForm, -- ADD NEW 28032022
    --    [?????a Ch??? KH] = ISNULL(cu.CustAddress, ''),
       --[M?? V??ng BH] = ISNULL(cu.Zone, ''),
       [T??n V??ng BH] = ISNULL(cu.ZoneDescr, ''),
       --[M?? Khu V???c] = ISNULL(cu.Territory, ''),
       [T??n Khu V???c] = ISNULL(cu.TerritoryDescr, ''),
       --[M?? T???nh KH] = ISNULL(cu.State, ''),
       [T??n T???nh KH] = ISNULL(cu.StateDescr, ''),
    --    [M?? Qu???n/HUy???n] = ISNULL(cu.District, ''),
        [T??n Qu???n/HUy???n] = ISNULL(cu.DistrictDescr, ''),
    --    [Ph?????ng/X??] = ISNULL(cu.WardDescr, ''),
       [M?? K??nh KH] = ISNULL(cu.Channel, ''),
       [T??n K??nh KH] = ISNULL(cu.ChannelDescr, ''),
       [M?? K??nh Ph???] = ISNULL(cu.ShopType, ''),
       [T??n K??nh Ph???] = ISNULL(cu.ShopTypeDescr, ''),
       [M?? HCO] = ISNULL(cu.HCOID, ''),
       [T??n HCO] = ISNULL(cu.HCOName, ''),
       [M?? Ph??n Lo???i HCO] = ISNULL(cu.HCOTypeID, ''),
       [T??n Ph??n Lo???i HCO] = ISNULL(cu.HCOTypeName, ''),
       [M?? Ph??n H???ng HCO] = ISNULL(cu.ClassId, ''),
       [T??n Ph??n H???ng HCO] = ISNULL(cu.ClassDescr, ''),
	   [Nh??n H??ng] =ISNULL(vih.NhanHangName,''),
       [M?? S???n Ph???m] = ISNULL(a.InvtID, ''),
       [T??n S???n Ph???m NB] = ISNULL(invt.Descr, ''),
       [T??n S???n Ph???m Vi???t T???t] = CASE
                                     WHEN ISNULL(invt.Descr1, '') = '' THEN
                                         ISNULL(invt.Descr, '')
                                     ELSE
                                         ISNULL(invt.Descr1, '')
                                 END,
       [S??? L??] = ISNULL(a.Lotsernbr, ''),
       [LineRef] = a.LineRef, -- #ADDNEW
       [S??? L?????ng] = (CASE
                         WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                             1
                         ELSE
                             -1
                     END
                    ) * ISNULL(a.OrdQty, 0),
       [????n Gi?? (C?? VAT)] = ISNULL(a.SlsPrice, 0),
       [Doanh S??? (C?? VAT)] = CASE
                                 WHEN a.FreeItem = 1 THEN
                                     0
                                 ELSE
       (CASE
            WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                1
            ELSE
                -1
        END
       ) * a.OrdQty * a.SlsPrice
                             END,
       [????n Gi?? (Ch??a VAT)] = ISNULL(a.BeforeVATPrice, 0),
       [Doanh S??? (Ch??a VAT)] = CASE
                                   WHEN a.FreeItem = 1 THEN
                                       0
                                   ELSE
       (CASE
            WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN
                1
            ELSE
                -1
        END
       ) * a.OrdQty * a.BeforeVATPrice
                               END,
       [Ng??y ?????t ??on] = ISNULL(a.Crtd_DateTime, ''),
    --    [Ng?????i T???o ????n] = ISNULL(cre.FirstName, ''),
       [Ng??y Giao H??ng] = CONVERT(VARCHAR(20), d.ShipDate, 103),
       [M?? NV] = LTRIM(RTRIM(ISNULL(a.SlsperID, ''))),
       [T??n CVBH] = ISNULL(sa.FirstName, ''),
	   [T??n Qu???n L?? TT] = ISNULL(sup.FirstName,''),
	   [T??n Qu???n L?? Khu V???c] = ISNULL(asm.FirstName,''),
	   [T??n Qu???n L?? V??ng] = ISNULL(Rsm.FirstName,''),
       [M?? NVGH] = ISNULL(iss.SlsperID, iss1.SlsperID),
       [Ng?????i Giao h??ng] = ISNULL(iss.Name, iss1.Name),
       [Tr???ng Th??i Giao H??ng] = CASE
                                    WHEN d.Status = 'H' THEN
                                        N'Ch??a x??c nh???n'
                                    WHEN d.Status = 'D' THEN
                                        N'KH Kh??ng nh???n'
                                    WHEN d.Status = 'A' THEN
                                        N'???? x??c nh???n'
                                    WHEN d.Status = 'R' THEN
                                        N'T??? Ch???i Giao H??ng'
                                    WHEN d.Status = 'C' THEN
                                        N'???? giao h??ng'
                                    WHEN d.Status = 'E' THEN
                                        N'Kh??ng ti???p t???c giao h??ng'
                                END,
    --    [S??? Xu???t H??ng] = iss.BatNbr,
        [????n V??? Giao H??ng] = ISNULL(iss.Descr, iss1.Descr), -- #ADDNEW Don Vi Giao Hang
        [T??n Nh?? V???n Chuy???n] = iss.DeliveryUnitName,
    --    [S??? Xe] = iss.TruckDescr,
    --    [Ng?????i Ch???u Tr??ch Nhi???m N???] = ISNULL(foll.FirstName, ''),
       [Ki???u ????n H??ng] = a.OrderType,
	   [M?? Kho]=a.SiteID,
	   [T??n Kho]=ist.Name

FROM #Ord a
    INNER JOIN #TCpnyID r WITH (NOLOCK)
        ON r.CpnyID = a.BranchID
    INNER JOIN dbo.OM_OrderType oo WITH (NOLOCK)
        ON oo.OrderType = a.OrderType
           AND ARDocType IN ( 'IN', 'DM', 'CS', 'CM' )
	INNER JOIN dbo.IN_Site ist WITH(NOLOCK) ON ist.SiteId=a.siteID
   LEFT JOIN dbo.vs_IN_Hierrachy vih WITH (NOLOCK) ON vih.InvtID = a.InvtID
    LEFT JOIN #SalesForce sf WITH (NOLOCK)
        ON sf.BranchID = a.BranchID
           AND sf.SlsperID = a.SlsperID
    LEFT JOIN #Doc b
        ON b.BranchID = a.BranchID
           AND b.CustId = a.CustID
           AND a.OrderNbr = b.OrderNbr
           AND a.InvcNbr = b.InvcNbr
           AND a.InvcNote = b.InvcNote

    --LEFT JOIN #TOrdDisc dis WITH (NOLOCK)
    --    ON dis.BranchID = a.BranchID
    --       AND dis.OrderNbr = a.MaCT
    --       AND dis.LineRef = a.LineRef
    --LEFT JOIN #TDiscFreeItem dis1 WITH (NOLOCK)
    --    ON dis1.BranchID = a.BranchID
    --       AND dis1.OrderNbr = a.MaCT
    --       AND dis1.FreeItemID = a.InvtID
    --       AND dis1.SOLineRef = a.LineRef
    LEFT JOIN dbo.SI_ReasonCode sr WITH (NOLOCK)
        ON sr.ReasonID = a.ReasonCode
    LEFT JOIN #Deli d
        ON d.BranchID = a.BranchID
           AND d.OrderNbr = a.OrderNbr
    LEFT JOIN #Book iss
        ON iss.BranchID = a.BranchID
           AND iss.OrderNbr = a.OrderNbr
    LEFT JOIN #Book iss1
        ON iss1.BranchID = a.BranchID
           AND iss1.OrderNbr = a.ReturnOrder
    INNER JOIN #Customer cu WITH (NOLOCK)
        ON cu.CustId = a.CustID
    INNER JOIN dbo.Users sa WITH (NOLOCK)
        ON sa.UserName = a.SlsperID
    INNER JOIN dbo.IN_Inventory invt WITH (NOLOCK)
        ON invt.InvtID = a.InvtID
    LEFT JOIN dbo.SYS_Company com WITH (NOLOCK)
        ON a.BranchID = com.CpnyID
    LEFT JOIN dbo.OM_DebtAllocateDet da WITH (NOLOCK)
        ON da.BranchID = a.BranchID
           AND da.OrderNbr = a.OrderNbr
           AND a.InvcNbr = da.InvcNbr
           AND a.InvcNote = da.InvcNote
    LEFT JOIN dbo.OM_OriginalContract con WITH (NOLOCK)
        ON con.ContractID = a.ContractID
    LEFT JOIN dbo.Users deli WITH (NOLOCK)
        ON deli.UserName = iss.SlsperID
    LEFT JOIN dbo.Users cre WITH (NOLOCK)
        ON cre.UserName = a.Crtd_User
    LEFT JOIN dbo.Users foll WITH (NOLOCK)
        ON foll.UserName = da.SlsperID
	LEFT JOIN dbo.Users sup WITH (NOLOCK) ON sup.UserName=a.SupID
	LEFT JOIN dbo.Users asm WITH (NOLOCK) ON asm.UserName=a.ASM
	LEFT JOIN dbo.Users Rsm WITH (NOLOCK) ON rsm.UserName =a.RSM
--where   (cu.Territory LIKE CASE WHEN @Terr = '' THEN '%' END OR cu.Territory IN (SELECT part FROM dbo.fr_SplitStringMAX(@Terr,',')))
--ORDER BY a.BranchID,
--         a.OrderDate,
--         ISNULL(a.InvcNbr, ''),
--         a.OrderNbr,
--         ISNULL(a.InvtID, ''),
--         ISNULL(a.Lotsernbr, '');



DROP TABLE #Doc;
DROP TABLE #Ord;
DROP TABLE #Deli;
DROP TABLE #SalesForce;
--DROP TABLE #DebtStatus;
DROP TABLE #Customer;
DROP TABLE #Book;
-- DROP TABLE #Sales;
--DROP TABLE #TOrdDisc;
--DROP TABLE #TDiscFreeItem;
DROP TABLE #TCpnyID;
DROP TABLE #TOrderType;
DROP TABLE #DataReturnIO
GO