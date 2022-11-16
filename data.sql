SELECT co.BranchID,
    COOrDer = co.OrderNbr,
    INOrderNbr = ino.OrderNbr,
    OrigOrderNbr = ino.OrigOrderNbr
-- INTO #WithOutOrderNbr
FROM biteam.sync_dms_so co
    INNER JOIN biteam.sync_dms_so ino
        ON ino.BranchID = co.BranchID
        AND co.InvcNbr = ino.InvcNbr
        AND co.InvcNote = ino.InvcNote
        AND ino.OrderDate = co.OrderDate
    -- INNER JOIN #TBranchID b WITH (NOLOCK)
        -- ON co.BranchID = b.BranchID
WHERE co.OrderType IN ( 'CO', 'HK' )
    AND ino.OrderType IN ( 'IN', 'IO', 'EP', 'NP' )
    AND co.Status = 'C'
    AND CAST(co.OrderDate AS DATE)
    BETWEEN '2022-01-01' AND '2022-01-01';