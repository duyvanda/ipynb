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
--where c.CustId in ('000061','000022')

