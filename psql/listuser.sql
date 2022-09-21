DECLARE @CurrentDate DATE,
        @UserID VARCHAR(30);
--SELECT @Fromdate='20210501',@Todate='20210909'
SELECT @CurrentDate = CAST(GETDATE() AS DATE),
       @UserID = 'Admin';

DECLARE @Fromdate DATE, @Todate DATE
SELECT @Fromdate='20210501',@Todate='20210909'

/* Lấy danh sách các ordertype cần thể hiện*/

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

DROP TABLE IF EXISTS #GroupDebtOwner
CREATE TABLE #GroupDebtOwner( Terms VARCHAR(10), PayMentForm VARCHAR(5), GroupID VARCHAR(10))
INSERT INTO #GroupDebtOwner
(
    Terms,
    PayMentForm,
    GroupID
)
VALUES
(   
'01',	'TM',	'MDS'),
('03',	'TM',	'MDS'),
('07',	'TM',	'CS'),
('10',	'TM',	'CS'),
('12',	'TM',	'CS'),
('15',	'TM',	'CS'),
('18',	'TM',	'CS'),
('20',	'TM',	'CS'),
('30',	'TM',	'CS'),
('45',	'TM',	'CS'),
('60',	'TM',	'CS'),
('90',	'TM',	'CS'),
('O1',	'TM',	'MDS'),
('O2',	'TM',	'CS'),
('O3',	'TM',	'CS'),
('01',	'CK',	'CS'),
('03',	'CK',	'CS'),
('07',	'CK',	'CS'),
('10',	'CK',	'CS'),
('12',	'CK',	'CS'),
('15',	'CK',	'CS'),
('18',	'CK',	'CS'),
('20',	'CK',	'CS'),
('30',	'CK',	'CS'),
('45',	'CK',	'CS'),
('60',	'CK',	'CS'),
('90',	'CK',	'CS'),
('O1',	'CK',	'CS'),
('O2',	'CK',	'CS'),
('O3',	'CK',	'CS')

    

DROP TABLE IF EXISTS #TSlsperID
SELECT *
INTO #TSlsperID
FROM dbo.fr_ListSaleByData(@UserID);

-- SELECT * FROM #GroupDebtOwner

DROP TABLE IF EXISTS #WithOutOrderNbr
SELECT co.BranchID,
       COOrDer = co.OrderNbr,
       INOrderNbr = ino.OrderNbr,
       OrigOrderNbr = ino.OrigOrderNbr
INTO #WithOutOrderNbr
FROM dbo.OM_SalesOrd co WITH (NOLOCK)
    INNER JOIN dbo.OM_SalesOrd ino WITH (NOLOCK)
        ON ino.BranchID = co.BranchID
           AND co.InvcNbr = ino.InvcNbr
           AND co.InvcNote = ino.InvcNote
           AND ino.OrderDate = co.OrderDate
WHERE co.OrderType IN ( 'CO', 'HK' )
      AND ino.OrderType IN ( 'IN', 'IO', 'EP', 'NP' )
      AND co.Status = 'C'
      AND CAST(co.OrderDate AS DATE)
      BETWEEN @Fromdate AND @Todate;