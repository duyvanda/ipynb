
SELECT
    d.BranchID,
    SlsperID = ISNULL(ib.SlsperID, deb.SlsperID),
    OrderNbr = d.OrigOrderNbr,
    OMOrder = sod.OrderNbr,
    DeliveryUnit = ISNULL(ib.DeliveryUnit, ''),
    sod.CustID,
    sod.InvoiceCustID,
    do.InvcNbr,
    do.InvcNote,
    sod.Version,
    OrderDate = CAST(d.ReturnDate AS DATE),
    DateOfOrder = CAST(sod.OrderDate AS DATE),
    DeliveryTime = '',
    TermsID = do.Terms,
    DueDate = CASE
                        WHEN do.Terms = 'O1' THEN
                            DATEADD(DAY, 30, do.DocDate)
                        ELSE
                            do.DueDate
                    END,
    OpeiningOrderAmt = 0,
    OrdAmtRelease = 0,
    DeliveredOrderAmt = 0,
    ReturnOrdAmt = do.OrigDocAmt,
    ReceiveAmt = 0,
    Reason = '',
    DebConfirmAmt = 0,
    DebConfirmAmtRelease = 0,
    sod.PaymentsForm
FROM
    (
        SELECT co.BranchID,
        ReturnDate = co.OrderDate,
        ino.OrderDate,
        COOrDer = co.OrderNbr,
        INOrderNbr = ino.OrderNbr,
        OrigOrderNbr = ino.OrigOrderNbr
    -- INTO #ReturnOrder
    FROM dbo.OM_SalesOrd co WITH (NOLOCK)
        INNER JOIN dbo.OM_SalesOrd ino WITH (NOLOCK)
        ON ino.BranchID = co.BranchID
            AND co.InvcNbr = ino.InvcNbr
            AND co.InvcNote = ino.InvcNote
            AND ino.OrderDate <> co.OrderDate
    -- INNER JOIN #TBranchID b WITH (NOLOCK)
    --     ON co.BranchID = b.BranchID
    WHERE co.OrderType IN ( 'CO', 'HK' )
        AND ino.OrderType IN ( 'IN', 'IO', 'EP', 'NP' )
        AND co.Status = 'C'
        ) as d
    INNER JOIN dbo.OM_PDASalesOrd ord WITH (NOLOCK)
    ON ord.BranchID = d.BranchID
        AND ord.OrderNbr = d.OrigOrderNbr
        AND ord.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
    INNER JOIN dbo.OM_SalesOrd sod WITH (NOLOCK)
    ON sod.BranchID = ord.BranchID
        AND sod.OrigOrderNbr = ord.OrderNbr
        AND sod.OrderNbr = d.INOrderNbr
    -- INNER JOIN #TOrderType ot WITH (NOLOCK)
    --     ON ot.OrderType = ord.OrderType
    INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
    ON deb.BranchID = d.BranchID
        AND deb.OrderNbr = d.OrigOrderNbr
        AND deb.ARBatNbr = sod.ARBatNbr
    LEFT JOIN dbo.OM_IssueBookDet bd WITH (NOLOCK)
    ON bd.BranchID = deb.BranchID
        AND bd.OrderNbr = deb.OrderNbr
    LEFT JOIN dbo.OM_IssueBook ib WITH (NOLOCK)
    ON ib.BranchID = bd.BranchID
        AND ib.BatNbr = bd.BatNbr
    INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
    ON sod.BranchID = do.BranchID
        AND sod.ARBatNbr = do.BatNbr
        AND sod.ARRefNbr = do.RefNbr
    INNER JOIN Batch b WITH (NOLOCK)
    ON do.BranchID = b.BranchID
        AND do.BatNbr = b.BatNbr
        AND b.Module = 'AR'
-- INNER JOIN #TBranchID br WITH (NOLOCK)
--     ON d.BranchID = br.BranchID
-- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
--     ON ts.BranchID = deb.BranchID
--        AND ts.SlsperID = deb.SlsperID
-- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
--     ON woo.BranchID = ord.BranchID
--        AND woo.OrigOrderNbr = ord.OrderNbr
WHERE CAST(d.ReturnDate AS DATE)
        BETWEEN '{Fromdate}' AND '{Todate}'
    AND sod.Status = 'C'
        --   AND woo.OrigOrderNbr IS NULL