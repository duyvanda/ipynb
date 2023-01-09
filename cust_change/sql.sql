select
Version,
1 as version_id,
ch.CustID,
ch.BranchID,
ch.SalesSystem,
ch.Channel,
ch.ShopType,
ch.HCOID,
ch.HCOType,
isnull(ch.ClassID, 'UnKnow') as ClassID,
ch.CheckTerm,
ch.LUpd_Datetime,
ch.LUpd_User,
cust.Crtd_Datetime,
cust.Crtd_User,
isnull(ch.CustName, 'UnKnow') as CustName,
ch.Addr1,
isnull(ch.Attn, 'UnKnow') as Attn,
isnull(ch.Country, 'UnKnow') as Country,
isnull(ch.District, 'UnKnow') as District,
isnull(ch.EMailAddr, 'UnKnow') as EMailAddr,
isnull(ch.Fax, 'UnKnow') as Fax,
isnull(ch.Phone, 'UnKnow') as Phone,
isnull(ch.SlsperId, 'UnKnow') as SlsperId,
isnull(ch.State, 'UnKnow') as State,
isnull(ch.Status, 'UnKnow') as Status,
isnull(ch.Terms, 'UnKnow') as Terms,
isnull(ch.Territory, 'UnKnow') as Territory,
isnull(ch.Zip, 'UnKnow') as Zip,
ISNULL(convert(varchar,ch.EstablishDate,23 ),'1900-01-01') as EstablishDate,
isnull(ch.RefCustID, 'UnKnow') as RefCustID,
isnull(ch.InActive, 'UnKnow') as InActive,
isnull(ch.Ward, 'UnKnow') as Ward,
isnull(ch.BusinessName, 'UnKnow') as BusinessName,
isnull(ch.Market, 'UnKnow') as Market,
isnull(ch.BillMarket, 'UnKnow') as BillMarket,
isnull(ch.OriCustID, 'UnKnow') as OriCustID,
isnull(ch.GeneralCustID, 'UnKnow') as GeneralCustID,
isnull(ch.PaymentsForm, 'UnKnow') as PaymentsForm,
isnull(ch.GenOrders, 'UnKnow') as GenOrders,
isnull(ch.BatchExpForm, 'UnKnow') as BatchExpForm,
isnull(ch.CustIdPublic, 'UnKnow') as CustIdPublic,
isnull(ch.ShoperID, 'UnKnow') as ShoperID,
isnull(ch.IsAgency, 'UnKnow') as IsAgency,
isnull(ch.AgencyID, 'UnKnow') as AgencyID,
isnull(ch.TaxDeclaration, 'UnKnow') as TaxDeclaration,
isnull(ch.StockSales, 'UnKnow') as StockSales,
isnull(ch.BusinessScope, 'UnKnow') as BusinessScope,
isnull(ch.LegalName, 'UnKnow') as LegalName,
ISNULL(convert(varchar,ch.LegalDate,23 ),'1900-01-01') as LegalDate,
isnull(ch.ChargeReceive, 'UnKnow') as ChargeReceive,
isnull(ch.ChargePayment, 'UnKnow') as ChargePayment,
isnull(ch.ChargePhar, 'UnKnow') as ChargePhar,
YEAR(cust.Crtd_Datetime) as Year_Created,
YEAR(ch.LUpd_Datetime) as Year_Updated,
ch.Addr2, --NO
ch.BillAddr1,--NO
ch.BillAddr2,--NO
ch.BillAttn,--NO
ch.BillCity,--NO
ch.BillCountry,--NO
ch.BillFax,--NO
ch.BillName,--NO
ch.BillPhone,--NO
ch.BillSalut,--NO
ch.BillState,--NO
ch.BillZip,--NO
ch.City,--NO
ch.CrLmt,--NO
ch.CrRule,--NO
ch.CustFillPriority,--NO
ch.CustType,--NO
ch.DeliveryID,--NO
ch.DflSaleRouteID,--NO
ch.DfltShipToId,--NO
ch.EmpNum,--NO
ch.ExpiryDate,--NO
ch.Exported,--NO
ch.GracePer,--NO
ch.LTTContractNbr,--NO
ch.NodeID,--NO
ch.NodeLevel,--NO
ch.ParentRecordID,--NO
ch.Phone,--NO
ch.PriceClassID,--NO
ch.Salut,--NO
ch.SiteId,--NO
ch.SupID,--NO
ch.TaxDflt,--NO
ch.TaxID00,--NO
ch.TaxID01,--NO
ch.TaxID02,--NO
ch.TaxID03,--NO
ch.TaxLocId,--NO
ch.TaxRegNbr,--NO
ch.TradeDisc,--NO
ch.Location,--NO
ch.Area,--NO
ch.GiftExchange,--NO
ch.HasPG,--NO
ch.Birthdate,--NO
ch.RefCustID,--NO
ch.SellProduct,--NO
ch.SearchName,--NO
ch.Classification,--NO
ch.Chain,--NO
ch.DeliveryUnit,--NO
ch.SalesProvince,--NO
ch.BusinessPic,--NO
ch.ProfilePic,--NO
ch.SubTerritory,--NO
ch.PhotoCode,--NO
ch.AllowEdit,--NO
ch.BillWard,--NO
ch.BillDistrict,--NO
ch.PPCPassword,--NO
ch.StandID,--NO
ch.BrandID,--NO
ch.DisplayID,--NO
ch.SizeID,--NO
ch.TypeCabinets,--NO
ch.OUnit,--NO
ch.Mobile,--NO
ch.LocationCheckType,--NO
ch.VendorID,--NO
ch.BuyerID,--NO
ch.GeneralCustID,--NO
ch.BillTerritory,--NO
ch.ToDate,--NO
ch.Limit,--NO
ch.ShoperID,--NO
ch.Account--NO
from AR_HistoryCustClassID ch
INNER JOIN dbo.AR_Customer cust WITH (NOLOCK) ON ch.CustID=cust.CustId
where cast(ch.LUpd_Datetime as date) <= '2022-12-31' order by LUpd_Datetime desc