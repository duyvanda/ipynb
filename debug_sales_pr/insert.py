import sys, os, csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.df_handle import *
import time
from memory_profiler import profile

def my_func():
    start=time.perf_counter()
    # query = f"EXEC [pr_OM_RawdataSellOutPayroll_BI]  @Fromdate='20210701', @Todate='20210731'"
    # start=time.perf_counter()
    # df1 = get_ms_df(sql=query)
    # # df1.to_csv('doanhthutienmat_data1.csv', index = False)
    # end=time.perf_counter()
    # df1.shape
    # server = '115.165.164.234'
    # driver = 'SQL Server'
    # db1 = 'PhaNam_eSales_PRO'
    # tcon = 'no'
    # uname = 'duyvq'
    # pword = '123VanQuangDuy'
    # cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', 
    #                       host=server, database=db1, trusted_connection=tcon,
    #                       user=uname, password=pword)
    # cursor = cnxn.cursor()
    day_ago = 2
    datenow = datetime.now().strftime("%Y%m%d")
    datenow_day_ago = ( datetime.now()-timedelta(day_ago) ).strftime("%Y%m%d")
    param_1 = f"'{datenow_day_ago}'"
    param_2 = f"'20210501'"
    param_3 = f"'20211015'"

    print(param_3)
    ORD = \
    f"""
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
        BETWEEN {param_2} AND {param_3}
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
        BETWEEN {param_2} AND {param_3}
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
    """
    # ORD = pd.read_sql(ORD, cnxn)
    get_ms_csv(ORD, 'ORD.csv')

    DATARETURNIO = f"""
    SELECT o.BranchID,
    o.OrigOrderNbr,
    o.OrderNbr
    FROM OM_SalesOrd o WITH (NOLOCK)
    INNER JOIN OM_SalesOrd o1 WITH (NOLOCK)
    ON o.BranchID = o1.BranchID
    AND o.InvcNbr = o1.InvcNbr
    AND o.InvcNote = o1.InvcNote
    AND o1.OrderType = 'IO'
    WHERE o.OrderDate
    BETWEEN {param_2} AND {param_3}
    AND o.OrderDate >= '20210501'
    AND o.OrderType IN ( 'CO', 'EP' )
    AND o.Status = 'C';
    """
    # DATARETURNIO = pd.read_sql(DATARETURNIO, cnxn)
    get_ms_csv(DATARETURNIO, 'DATARETURNIO.csv')
    # DATARETURNIO.head()
    # ORD.head()
    # ORD.shape
    # vc(ORD,'OrderType')
    # ORD.shape
    # ORD = ORD.merge(DATARETURNIO, how = 'inner', left_on=['BranchID', 'OrderNbr'], right_on=['BranchID', 'OrderNbr'], suffixes=('_ORD', '_DATARETURNIO'))
    DELI = """
    SELECT DISTINCT
    a.BranchID,
    a.BatNbr,
    a.SlsperID,
    a.Status,
    a.OrderNbr,
    ShipDate = ISNULL(c.ShipDate, a.Crtd_DateTime)
    -- INTO #Deli
    FROM OM_Delivery a WITH (NOLOCK)

    --INNER JOIN #Sales d ON 
    --d.BranchID = a.BranchID AND
    --d.OrderNbr = a.OrderNbr
    INNER JOIN
    (
    SELECT
    de.BranchID,
    de.BatNbr,
    de.OrderNbr,
    Sequence = MAX(Sequence)
    FROM dbo.OM_Delivery de
    -- INNER JOIN #Sales d
    -- ON d.BranchID = de.BranchID
    -- AND d.OrderNbr = de.OrderNbr
    GROUP BY 
    de.BranchID,
    de.BatNbr,
    de.OrderNbr
    ) b ON
    b.BatNbr = a.BatNbr AND 
    b.BranchID = a.BranchID AND 
    b.Sequence = a.Sequence AND 
    b.OrderNbr = a.OrderNbr
    LEFT JOIN
    (
    SELECT de.BranchID,
    de.BatNbr,
    de.OrderNbr,
    ShipDate = MAX(ShipDate)
    FROM dbo.OM_DeliHistory de
    -- INNER JOIN #Sales d ON
    -- d.BranchID = de.BranchID AND
    -- d.OrderNbr = de.OrderNbr
    GROUP BY
    de.BranchID,
    de.BatNbr,
    de.OrderNbr
    ) c ON
    c.BatNbr = a.BatNbr AND
    c.BranchID = a.BranchID AND
    c.OrderNbr = a.OrderNbr;
    """
    # DELI = pd.read_sql(DELI, cnxn)
    get_ms_csv(DELI, 'DELI.csv')
    # vc(DELI, ['ShipDate'])
    BOOK ="""
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
    """
    # BOOK = pd.read_sql(BOOK, cnxn)
    get_ms_csv(BOOK, 'BOOK.csv')
    CUSTOMER = \
    """
    SELECT DISTINCT
        c.CustId,
        c.CustName,
        c.BranchID,
        c.RefCustID,
        PubCustID = pc.PubCust,
        PubCustName = pc.CustName,
        c.TaxRegNbr,
        c.Attn,
        te.Zone,
        ZoneDescr = z.Descr,
        c.Territory,
        TerritoryDescr = te.Descr,
        c.State,
        StateDescr = st.Descr,
        c.SalesSystem,
        SalesSystemDescr = st.Descr,
        c.Channel,
        ChannelDescr = ch.Descr,
        c.ShopType,
        ShopTypeDescr = sh.Descr,
        c.HCOID,
        hc.HCOName,
        c.HCOTypeID,
        ht.HCOTypeName,
        c.ClassId,
        ClassDescr = cu.Descr,
        c.Terms,
        TermDescr = tm.Descr,
        c.ShoperID,
        c.GenOrders,
        GenOrdersDescr = g.Descr,
        c.BatchExpForm,
        BatchExpFormDescr = f.Descr,
        c.CheckTerm,
        CheckTermDescr = ct.Descr,
        c.PaymentsForm,
        PaymentsFormDescr = mp.Descr,
        c.Account,
        a.AcctName,                                                                 --CustAddress = c.Addr1 ,
        c.District,
        DistrictDescr = di.Name,
        c.Ward,
        WardDescr = w.Name,
        c.Phone,
        c.Limit,
        inv.CustIDInvoice,
        ci.CustNameInvoice,
        ci.TaxID,                                                                   --- Ngochb them
        CustInvoiceAddr = ci.ApartNumber + CASE WHEN ISNULL(ci.StreetName,'') ='' THEN '' ELSE ISNULL(' ' + ci.StreetName, '') end --- + Tên Đường
                            + ISNULL(', ' + NULLIF(wa.Name, ''), '') --- + phường Xã
                            + ISNULL(', ' + NULLIF(ISNULL(Dis.[Name], ''), ''), '') ---- + Quận Huyện
                            + ISNULL(', ' + NULLIF(ISNULL(   CASE
                                                                WHEN Sta.Code = '1' THEN
                                                                    Sta.[Descr]
                                                                ELSE
                                                                    CASE
                                                                        WHEN Sta.State = '28' THEN
                                                                            N'Thành phố '
                                                                        ELSE
                                                                            N'Tỉnh '
                                                                    END + Sta.[Descr]
                                                            END,
                                                            ''
                                                        ), ''),
                                    ''
                                    ) ---- + Tỉnh/Thành Phố
                            + ISNULL(', ' + NULLIF(ISNULL(stc.[Descr], ''), ''), ''), ---- + Đất Nước
        CustAddress = ISNULL(c.Addr1 + ', ', '') --+ ISNULL(c.Addr2 + ', ', '')
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
                                                                                    END + ISNULL(st.Descr, '')
    --c.Addr1	-- + ISNULL(' ' + ci.StreetName, '') --- + Tên Đường
    --							+ ISNULL(', ' + NULLIF(w.Name,''), '') --- + phường Xã
    --							+ ISNULL(', ' + NULLIF(ISNULL(Di.[Name], ''),''), '') ---- + Quận Huyện
    --							+ ISNULL(', ' + NULLIF(ISNULL(St.[Descr], ''),''), '') ---- + Tỉnh/Thành Phố
    FROM dbo.AR_Customer c WITH (NOLOCK)
        LEFT JOIN dbo.SI_Territory te WITH (NOLOCK)
            ON te.Territory = c.Territory
        LEFT JOIN dbo.AR_HCO hc WITH (NOLOCK)
            ON hc.HCOID = c.HCOID
        LEFT JOIN dbo.AR_HCOType ht WITH (NOLOCK)
            ON ht.HCOTypeID = c.HCOTypeID
        LEFT JOIN dbo.AR_PublicCust pc WITH (NOLOCK)
            ON pc.PubCust = c.CustIdPublic
        LEFT JOIN dbo.AR_Channel ch WITH (NOLOCK)
            ON c.Channel = ch.Code
        LEFT JOIN dbo.AR_ShopType sh WITH (NOLOCK)
            ON sh.Code = c.ShopType
        LEFT JOIN dbo.SI_Terms tm WITH (NOLOCK)
            ON tm.TermsID = c.Terms
        LEFT JOIN dbo.AR_MasterAutoGenOrder g WITH (NOLOCK)
            ON g.Code = c.GenOrders
        LEFT JOIN dbo.AR_MasterBatchExpForm f WITH (NOLOCK)
            ON f.Code = c.BatchExpForm
        LEFT JOIN dbo.AR_MasterCheckTerm ct WITH (NOLOCK)
            ON ct.Code = c.CheckTerm
        LEFT JOIN dbo.AR_MasterPayments mp WITH (NOLOCK)
            ON mp.Code = c.PaymentsForm
        LEFT JOIN dbo.CA_Account a WITH (NOLOCK)
            ON a.AcctNbr = c.Account
            AND a.BranchID = c.BranchID
        LEFT JOIN dbo.AR_CustClass cu WITH (NOLOCK)
            ON cu.ClassId = c.ClassId
        LEFT JOIN dbo.SYS_SalesSystem sy WITH (NOLOCK)
            ON c.SalesSystem = sy.Code
        LEFT JOIN dbo.SI_Zone z WITH (NOLOCK)
            ON z.Code = te.Zone
        LEFT JOIN dbo.SI_State st WITH (NOLOCK)
            ON st.State = c.State
        LEFT JOIN dbo.SI_District di WITH (NOLOCK)
            ON c.District = di.District
            AND di.State = st.State
        LEFT JOIN dbo.SI_Ward w WITH (NOLOCK)
            ON w.Ward = c.Ward
            AND w.District = di.District
            AND di.State = st.State
        LEFT JOIN dbo.AR_Customer_InvoiceCustomer inv WITH (NOLOCK)
            ON inv.CustID = c.CustId
            AND inv.Active = 1 --PhucPM thêm KH XHĐ 
        LEFT JOIN dbo.AR_CustomerInvoice ci WITH (NOLOCK)
            ON ci.CustIDInvoice = inv.CustIDInvoice --PhucPM thêm KH XHĐ 
        LEFT JOIN SI_Ward wa WITH (NOLOCK)
            ON ci.Ward = wa.Ward
            AND ci.DistrictID = wa.District
            AND ci.[State] = wa.[State]
        LEFT JOIN SI_District Dis WITH (NOLOCK)
            ON ci.DistrictID = Dis.District
            AND ci.[State] = Dis.[State]
        LEFT JOIN SI_State Sta WITH (NOLOCK)
            ON ci.[State] = Sta.[State]
        LEFT JOIN SI_Country stc WITH (NOLOCK)
            ON stc.CountryID = Sta.Country;
    """
    # CUSTOMER = pd.read_sql(CUSTOMER, cnxn)
    get_ms_csv(CUSTOMER, 'CUSTOMER.csv')
    IN_INVENTORY = """
    SELECT
    InvtID,
    Descr,
    Descr1
    FROM dbo.IN_Inventory;
    """
    # IN_INVENTORY = pd.read_sql(IN_INVENTORY, cnxn)
    get_ms_csv(IN_INVENTORY, 'IN_INVENTORY.csv')
    USERS = """
    SELECT *
    FROM dbo.Users;
    """
    # USERS = pd.read_sql(USERS, cnxn)
    get_ms_csv(USERS, 'USERS.csv')
    COMPANY = """
    SELECT
    CpnyID,
    CpnyName
    FROM dbo.SYS_Company;
    """
    # COMPANY = pd.read_sql(COMPANY, cnxn)
    get_ms_csv(COMPANY, 'COMPANY.csv')
    ORDERTYPE = """
    select
    OrderType,
    ARDocType
    FROM OM_OrderType
    """
    # ORDERTYPE = pd.read_sql(ORDERTYPE, cnxn)
    get_ms_csv(ORDERTYPE, 'ORDERTYPE.csv')
    # DELI.columns
    # DELI['ShipDate']
    ORD=pd.read_csv('ORD.csv')
    DATARETURNIO=pd.read_csv('DATARETURNIO.csv')
    DELI=pd.read_csv('DELI.csv')
    BOOK=pd.read_csv('BOOK.csv')
    CUSTOMER=pd.read_csv('CUSTOMER.csv')
    IN_INVENTORY=pd.read_csv('IN_INVENTORY.csv')
    USERS=pd.read_csv('USERS.csv')
    COMPANY=pd.read_csv('COMPANY.csv')
    ORDERTYPE=pd.read_csv('ORDERTYPE.csv')
    DATARETURNIO
    ORD.shape
    ORD = ORD[ORD['OrderType'] != 'IO']
    ORD.OrderDate
    FINAL = ORD.merge(DELI.add_prefix('DELI_'), how = 'left', left_on=['BranchID', 'OrderNbr'], right_on=['DELI_BranchID', 'DELI_OrderNbr'])
    ORD.shape
    # FINAL.shape
    FINAL.columns
    FINAL = FINAL.merge(USERS[['UserName','FirstName']].add_prefix('SUP_'), how='left', left_on='SupID', right_on=['SUP_UserName'])
    FINAL.columns
    FINAL = FINAL.merge(USERS[['UserName','FirstName']].add_prefix('ASM_'), how='left', left_on='ASM', right_on=['ASM_UserName'])
    FINAL = FINAL.merge(USERS[['UserName','FirstName']].add_prefix('RSM_'), how='left', left_on='RSM', right_on=['RSM_UserName'])
    FINAL.columns
    FINAL = FINAL.merge(USERS[['UserName','FirstName']].add_prefix('SA_'), how='left', left_on='SlsperID', right_on=['SA_UserName'])
    FINAL.columns
    BOOK.columns
    BOOK=BOOK[['BranchID', 'OrderNbr', 'SlsperID','Name','Descr','DeliveryUnitName']]
    FINAL = FINAL.merge(BOOK.add_prefix('BOOK_'), how='left', left_on=['BranchID','OrderNbr'], right_on=['BOOK_BranchID','BOOK_OrderNbr'])
    FINAL.columns
    # FINAL_3 = FINAL[['BranchID','OrderNbr']]
    # FINAL_3.to_clipboard()
    # FINAL_2 = FINAL[['BranchID','ReturnOrder']]
    # FINAL_2.to_clipboard()
    # BOOK_2 = FINAL[['BranchID','OrderNbr']]
    BOOK = df_notna(BOOK, 'OrderNbr')
    # FINAL_3 = FINAL_2.merge(BOOK.add_prefix('BOOK_RT_'), how='left', left_on=['BranchID','ReturnOrder'], right_on=['BOOK_RT_BranchID','BOOK_RT_OrderNbr'])
    # FAIL
    FINAL = FINAL.merge(BOOK.add_prefix('BOOK_RT_'), how='left', left_on=['BranchID','ReturnOrder'], right_on=['BOOK_RT_BranchID','BOOK_RT_OrderNbr'])
    FINAL.columns
    # FINAL[['BranchID','ReturnOrder','BOOK_RT_BranchID','BOOK_RT_OrderNbr']].to_clipboard()
    # FINAL.shape
    # FINAL.columns
    FINAL = FINAL.merge(CUSTOMER.add_prefix('CUSTOMER_'), how='left', left_on=['CustID'], right_on='CUSTOMER_CustId')
    FINAL = FINAL.merge(COMPANY.add_prefix('COMPANY_'), how='left', left_on=['BranchID'], right_on='COMPANY_CpnyID')
    FINAL.columns
    FINAL = FINAL.merge(IN_INVENTORY.add_prefix('INVT_'), how='inner', left_on=['InvtID'], right_on='INVT_InvtID')
    FINAL = FINAL.merge(ORDERTYPE.add_prefix('ORDERTYPE_'), how='inner', left_on=['OrderType'], right_on='ORDERTYPE_OrderType')
    unwanted_cols = [
    'MaCT',
    'ExpDate',
    'CustID',
    'VATAmount',
    'BeforeVATAmount',
    'AfterVATAmount',
    'Crtd_User',
    'ContractID',
    'DeliveryID',
    'ShipDate',
    'OrdAmt',
    'InvcNote',
    'ChietKhau',
    'ContractNbr',
    # 'LineRef',
    'ReasonCode',
    'SupID',
    'ASM',
    'RSM',
    'DELI_BranchID',
    'DELI_BatNbr',
    'DELI_SlsperID',
    'DELI_OrderNbr',
    'SUP_UserName',
    'ASM_UserName',
    'RSM_UserName',
    'SA_UserName',
    'BOOK_BranchID',
    'BOOK_OrderNbr',
    'BOOK_RT_BranchID',
    'BOOK_RT_OrderNbr',
    'CUSTOMER_CustId',
    'CUSTOMER_CustName',
    'CUSTOMER_BranchID',
    'CUSTOMER_PubCustID',
    'CUSTOMER_PubCustName',
    'CUSTOMER_TaxRegNbr',
    'CUSTOMER_Attn',
    'CUSTOMER_SalesSystem',
    'CUSTOMER_SalesSystemDescr',
    'CUSTOMER_ClassId',
    'CUSTOMER_ClassDescr',
    'CUSTOMER_Terms',
    'CUSTOMER_TermDescr',
    'CUSTOMER_ShoperID',
    'CUSTOMER_GenOrders',
    'CUSTOMER_GenOrdersDescr',
    'CUSTOMER_BatchExpForm',
    'CUSTOMER_BatchExpFormDescr',
    'CUSTOMER_CheckTerm',
    'CUSTOMER_CheckTermDescr',
    'CUSTOMER_PaymentsForm',
    'CUSTOMER_PaymentsFormDescr',
    'CUSTOMER_Account',
    'CUSTOMER_AcctName',
    'CUSTOMER_District',
    'CUSTOMER_DistrictDescr',
    'CUSTOMER_Ward',
    'CUSTOMER_WardDescr',
    'CUSTOMER_Phone',
    'CUSTOMER_Limit',
    'CUSTOMER_CustIDInvoice',
    'CUSTOMER_CustNameInvoice',
    'CUSTOMER_TaxID',
    'CUSTOMER_CustInvoiceAddr',
    'CUSTOMER_CustAddress',
    'COMPANY_CpnyID',
    'COMPANY_CpnyName',
    'INVT_InvtID',
    'ORDERTYPE_OrderType'
    ]
    cols = FINAL.columns
    cols = [ele for ele in cols if ele not in unwanted_cols]
    FINAL=FINAL[cols]
    FINAL.columns
    cols =[
    'MaCongTyCN',
    'SoDonDatHang',
    'MaNV',
    'NgayChungTu',
    'SoDonTraHang',
    'NgayTraHang',
    'MaSanPham',
    'SoLo',
    'TrangThai',
    'NgayTaoDon',
    'SoLuong',
    'HoaDon',
    'KieuDonHang',
    'DonGiaCoVAT',
    'DonGiaChuaVAT',
    'FreeItem',
    'LineRef',
    'TrangThaiGiaoHang',
    'NgayGiaoHang',
    'TenQuanLyTT',
    'TenQuanLyKhuVuc',
    'TenQuanLyVung',
    'TenCVBH',
    'MaNVGH',
    'NguoiGiaoHang',
    'DonViGiaoHang',
    'TenNhaVanChuyen',
    'RT_MaNVGH',
    'RT_NguoiGiaoHang',
    'RT_DonViGiaoHang',
    'RT_TenNhaVanChuyen',
    'MaKHCu',
    'MaVungBH',
    'TenVungBH',
    'MaKhuVuc',
    'TenKhuVuc',
    'MaTinhKH',
    'TenTinhKH',
    'MaKenhKH',
    'TenKenhKH',
    'MaKenhPhu',
    'TenKenhPhu',
    'MaHCO',
    'TenHCO',
    'MaPhanLoaiHCO',
    'TenPhanLoaiHCO',
    'TenSanPhamNoiBo',
    'TenSanPhamVietTat',
    'ARDocType'
    ]
    FINAL.columns = cols
    # FINAL.head()
    FINAL['TenSanPhamVietTat'] = np.where(FINAL['TenSanPhamVietTat'].str.contains(r'^\s*$', regex=True), FINAL['TenSanPhamNoiBo'], FINAL['TenSanPhamVietTat'])
    FINAL.columns
    FINAL['SoLuong'] = np.where(FINAL['ARDocType'].isin(['IN', 'DM', 'CS']), 1 * FINAL['SoLuong'], -1 * FINAL['SoLuong'])
    FINAL['DoanhSoChuaVAT'] = np.where(FINAL['FreeItem'], 0, FINAL['SoLuong']*FINAL['DonGiaChuaVAT'])
    FINAL['DoanhSoChuaVAT'].sum()

    # vc(FINAL, ['MaNVGH']).to_clipboard()
    FINAL['MaNVGH'] = np.where(FINAL['MaNVGH'].str.contains(r'^\s*$', regex=True), FINAL['RT_MaNVGH'], FINAL['MaNVGH'])
    FINAL['NguoiGiaoHang'] = np.where(FINAL['NguoiGiaoHang'].str.contains(r'^\s*$', regex=True), FINAL['RT_NguoiGiaoHang'], FINAL['NguoiGiaoHang'])
    FINAL['DonViGiaoHang'] = np.where(FINAL['DonViGiaoHang'].str.contains(r'^\s*$', regex=True), FINAL['RT_DonViGiaoHang'], FINAL['DonViGiaoHang'])
    FINAL['TenNhaVanChuyen'] = np.where(FINAL['TenNhaVanChuyen'].str.contains(r'^\s*$', regex=True), FINAL['RT_TenNhaVanChuyen'], FINAL['TenNhaVanChuyen'])
    unwanted_cols = ['BOOK_RT_SlsperID',
    'BOOK_RT_Name',
    'BOOK_RT_Descr',
    'BOOK_RT_DeliveryUnitName',
    'RT_MaNVGH',
    'RT_NguoiGiaoHang',
    'RT_DonViGiaoHang',
    'RT_TenNhaVanChuyen'
    ]
    cols = FINAL.columns
    cols = [ele for ele in cols if ele not in unwanted_cols]
    FINAL=FINAL[cols]
    # DATARETURNIO.head()
    set_DATARETURNIO = set(DATARETURNIO['BranchID']+DATARETURNIO['OrigOrderNbr'])
    set_DATARETURNIO
    # set(DATARETURNIO['BO'])
    # FINAL = (FINAL['MaCongTyCN']+FINAL['SoDonDatHang']).isin(set_DATARETURNIO)
    df_in_set = (FINAL['MaCongTyCN']+FINAL['SoDonDatHang']).isin(set_DATARETURNIO)
    df_in_set.sum()
    # df_in_set
    FINAL = FINAL[~df_in_set]
    FINAL.columns
    # FINAL['NgayGiaoHang'] = FINAL['NgayGiaoHang'].str[:10]
    # vc(FINAL, 'NgayGiaoHang_2').to_clipboard()
    FINAL['NgayGiaoHang']=pd.to_datetime(FINAL['NgayGiaoHang'].str[:10], dayfirst=True)
    FINAL['NgayChungTu']=pd.to_datetime(FINAL['NgayChungTu'].str[:10], dayfirst=True)
    # FINAL.to_clipboard()
    # FINAL['DoanhSoChuaVAT'].sum()
    FINAL.shape
    FINAL['DoanhSoChuaVAT'].sum()
    checkdup(FINAL, 1, ['MaCongTyCN','SoDonDatHang','NgayChungTu','MaSanPham','SoLuong','SoLo','LineRef']).sum()
    # print_df_schema(FINAL)
    # FINAL['NgayChungTu'] = FINAL['NgayChungTu'].str[:10]
    # FINAL['NgayChungTu'].head()
    # FINAL['NgayChungTu_2']=pd.to_datetime(FINAL['NgayChungTu'], dayfirst=True)
    # FINAL['NgayChungTu_2'].head()
    # raw_usecols_list = [' Ngày Chứng Từ', ' Mã Công Ty/CN', ' Số Đơn Đặt Hàng', ' Mã Sản Phẩm', ' Số Lô', ' Số Lượng', ' Đơn Giá (Chưa VAT)', ' Số Đơn Trả Hàng', ' Doanh Số (Chưa VAT)']
    # raw_usecols_list = [' Ngày Chứng Từ', ' Mã Công Ty/CN', ' Số Đơn Đặt Hàng', ' Mã Sản Phẩm', ' Số Lượng', ' Đơn Giá (Chưa VAT)', ' Số Đơn Trả Hàng', ' Doanh Số (Chưa VAT)']
    # df1 = pd.read_csv('Rawdata_t7.csv', usecols=raw_usecols_list, nrows=None, dayfirst=True, parse_dates=[' Ngày Chứng Từ'])
    # print(df1.shape)
    # df1.columns=cleancols(df1)
    # df1.drop_duplicates(keep='first', inplace=True)
    # df1.shape
    # df1['NgayChungTu'].head()
    # df1_c = df1[['MaCongTyCN','NgayChungTu', '']]
    # lo(9)
    # FINAL.columns
    # final_c = FINAL[['MaCongTyCN', 'NgayChungTu',  'SoDonDatHang', 'SoDonTraHang', 'MaSanPham', 'SoLuong', 'DonGiaChuaVAT', 'DoanhSoChuaVAT']].copy()
    # final_c['SoLuong'] = final_c['SoLuong'].astype('int64')
    # final_c['DonGiaChuaVAT'] = final_c['DonGiaChuaVAT'].astype('int64')
    # final_c['DoanhSoChuaVAT'] = final_c['SoLuong'].astype('int64')
    # final_c.dtypes
    # final_d = final_c[['MaCongTyCN', 'SoDonDatHang','MaSanPham']]
    # final_d = dropdup(final_d,1)
    # df1_d = df1[['MaCongTyCN', 'SoDonDatHang','MaSanPham']]
    # df1_d = dropdup(df1_d,1)
    # dfs_diff(final_d, df1_d)
    # set_DATARETURNIO
    # df_filter(ORD, BranchID='MR0001', OrderNbr='CO072021-00001').to_clipboard()

    pk_list = ['macongtycn','sodondathang','ngaychungtu','masanpham','soluong','solo','lineref']
    # for l in pk_list:
    #     print(l)
    # FINAL2 =FINAL.head()

    # insert_df_to_postgres("f_sales", FINAL, pk_list)
    FINAL['NgayGiaoHang'].fillna(datetime(1900,1,1), inplace=True)

    rows = [tuple(x) for x in FINAL.to_numpy()]
    insert_rows_psql(table='f_sales', rows=rows, replace=True, target_fields=[x.lower() for x in FINAL.columns], replace_index=pk_list)

    end=time.perf_counter()
    duration=end-start
    print(duration)

if __name__=='__main__':
  my_func()