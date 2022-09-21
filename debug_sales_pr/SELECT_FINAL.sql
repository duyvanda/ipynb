SELECT
[Mã Công Ty/CN] = ISNULL(a.BranchID, ''),
[Công Ty/CN] = ISNULL(com.CpnyName, ''),
[Ngày Chứng Từ] = ISNULL(a.OrderDate, ''),
[Số Đơn Đặt Hàng] = ISNULL(a.OrderNbr, ''),
[Số Đơn Trả Hàng] = ISNULL(a.ReturnOrder, ''),
[Ngày Trả Hàng] = ISNULL(CONVERT(VARCHAR(10), a.ReturnOrderdate, 103), ''),
[Hóa Đơn] = ISNULL(a.InvcNbr, ''),
-- [Ngày Tới Hạn TT] = ISNULL(CONVERT(VARCHAR(10), b.DueDate, 103), ''),
-- [Số Hợp Đồng] = ISNULL(con.ContractNbr, ''),
[Trạng Thái] = a.Status,
-- [Mã KH Thuế] = ISNULL(cu.CustIDInvoice, ''),
-- [Tên KH Thuế] = ISNULL(cu.CustNameInvoice, ''),
-- [Địa Chỉ KH Thuế] = ISNULL(cu.CustInvoiceAddr, ''),
-- [Mã Số Thuế] = ISNULL(cu.TaxID, ''),
-- [Mã KH DMS] = ISNULL(a.CustID, ''),
[Mã KH Cũ] = ISNULL(cu.RefCustID, ''),
-- [Tên Khách Hàng] = ISNULL(cu.CustName, ''),
-- [Địa Chỉ KH] = ISNULL(cu.CustAddress, ''),
[Mã Vùng BH] = ISNULL(cu.Zone, ''),
[Tên Vùng BH] = ISNULL(cu.ZoneDescr, ''),
[Mã Khu Vực] = ISNULL(cu.Territory, ''),
[Tên Khu Vực] = ISNULL(cu.TerritoryDescr, ''),
[Mã Tỉnh KH] = ISNULL(cu.State, ''),
[Tên Tỉnh KH] = ISNULL(cu.StateDescr, ''),
-- [Mã Quận/HUyện] = ISNULL(cu.District, ''),
-- [Tên Quận/HUyện] = ISNULL(cu.DistrictDescr, ''),
-- [Phường/Xã] = ISNULL(cu.WardDescr, ''),
[Mã Kênh KH] = ISNULL(cu.Channel, ''),
[Tên Kênh KH] = ISNULL(cu.ChannelDescr, ''),
[Mã Kênh Phụ] = ISNULL(cu.ShopType, ''),
[Tên Kênh Phụ] = ISNULL(cu.ShopTypeDescr, ''),
[Mã HCO] = ISNULL(cu.HCOID, ''),
[Tên HCO] = ISNULL(cu.HCOName, ''),
[Mã Phân Loại HCO] = ISNULL(cu.HCOTypeID, ''),
[Tên Phân Loại HCO] = ISNULL(cu.HCOTypeName, ''),
[Mã Phân Hạng HCO] = ISNULL(cu.ClassId, ''),
[Tên Phân Hạng HCO] = ISNULL(cu.ClassDescr, ''),
[Mã Sản Phẩm] = ISNULL(a.InvtID, ''),
--look agian
[Tên Sản Phẩm NB] = ISNULL(invt.Descr, ''),
[Tên Sản Phẩm Viết Tắt] = 
CASE
    WHEN ISNULL(invt.Descr1, '') = '' THEN ISNULL(invt.Descr, '')
    ELSE ISNULL(invt.Descr1, '')
END,

[Số Lô] = ISNULL(a.Lotsernbr, ''),
[Số Lượng] = 
(CASE
    WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
    ELSE -1
END) * ISNULL(a.OrdQty, 0),
[Đơn Giá (Có VAT)] = ISNULL(a.SlsPrice, 0),
[Doanh Số (Có VAT)] = 
CASE
    WHEN a.FreeItem = 1 THEN 0
    ELSE
        (CASE
            WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
            ELSE -1
        END) * a.OrdQty * a.SlsPrice
END,
[Đơn Giá (Chưa VAT)] = ISNULL(a.BeforeVATPrice, 0),
[Doanh Số (Chưa VAT)] = 
CASE
    WHEN a.FreeItem = 1 THEN 0
    ELSE
        (CASE
            WHEN oo.ARDocType IN ( 'IN', 'DM', 'CS' ) THEN 1
            ELSE -1
        END) * a.OrdQty * a.BeforeVATPrice
END,
[Ngày Đặt Đon] = ISNULL(a.Crtd_DateTime, ''),
-- [Người Tạo Đơn] = ISNULL(cre.FirstName, ''),
[Ngày Giao Hàng] = ISNULL(CONVERT(VARCHAR(20), d.ShipDate, 103), ''),
[Mã NV] = ISNULL(a.SlsperID, ''),
[Tên CVBH] = ISNULL(sa.FirstName, ''),
[Tên Quản Lý TT] = ISNULL(sup.FirstName,''),
[Tên Quản Lý Khu Vực] = ISNULL(asm.FirstName,''),
[Tên Quản Lý Vùng] = ISNULL(Rsm.FirstName,''),
[Mã NVGH] = ISNULL(iss.SlsperID, iss1.SlsperID),
[Người Giao hàng] = ISNULL(iss.Name, iss1.Name),
[Trạng Thái Giao Hàng] =
CASE
    WHEN d.Status = 'H' THEN N'Chưa xác nhận'
    WHEN d.Status = 'D' THEN N'KH Không nhận'
    WHEN d.Status = 'A' THEN N'Đã xác nhận'
    WHEN d.Status = 'R' THEN N'Từ Chối Giao Hàng'
    WHEN d.Status = 'C' THEN N'Đã giao hàng'
    WHEN d.Status = 'E' THEN N'Không tiếp tục giao hàng'
END,
-- [Sổ Xuất Hàng] = iss.BatNbr,
[Đơn Vị Giao Hàng] = iss.Descr,
[Tên Nhà Vận Chuyển] = iss.DeliveryUnitName,
-- [Số Xe] = iss.TruckDescr,
-- [Người Chịu Trách Nhiệm Nợ] = ISNULL(foll.FirstName, ''),
[Kiểu Đơn Hàng] = a.OrderType
-- [Mã Lý Do] = sr.ProgramID,
-- [Mã CSBH] = 
-- CASE
--     WHEN ISNULL(dis.TypeDiscount, '') = 'SP' THEN ISNULL(dis.DiscIDPN, '')
--     WHEN ISNULL(dis1.TypeDiscount, '') = 'SP' THEN ISNULL(dis1.DiscIDPN, '')
--     ELSE ''
-- END,
-- [Tên CSBH] = 
-- CASE
--     WHEN ISNULL(dis.TypeDiscount, '') = 'SP' THEN ISNULL(dis.Descr, '')
--     WHEN ISNULL(dis1.TypeDiscount, '') = 'SP' THEN ISNULL(dis1.Descr, '')
-- ELSE ''
-- END,
-- [Mã CTKM] = 
-- CASE
--     WHEN ISNULL(dis.TypeDiscount, '') = 'PR' THEN ISNULL(dis.DiscIDPN, '')
--     WHEN ISNULL(dis1.TypeDiscount, '') = 'PR' THEN ISNULL(dis1.DiscIDPN, '')
--     ELSE ''
-- END,
-- [Tên CTKM] = 
-- CASE
--     WHEN ISNULL(dis.TypeDiscount, '') = 'PR' THEN ISNULL(dis.Descr, '')
--     WHEN ISNULL(dis1.TypeDiscount, '') = 'PR' THEN ISNULL(dis1.Descr, '')
--     ELSE ''
-- END,
-- [Mã CTTL] = 
-- CASE
--     WHEN ISNULL(dis.TypeDiscount, '') = 'AC' THEN ISNULL(dis.DiscIDPN, '')
--     WHEN ISNULL(dis1.TypeDiscount, '') = 'AC' THEN ISNULL(dis1.DiscIDPN, '')
--     ELSE ''
-- END,
-- [Tên CTTL] = 
-- CASE
--     WHEN ISNULL(dis.TypeDiscount, '') = 'AC' THEN ISNULL(dis.Descr, '')
--     WHEN ISNULL(dis1.TypeDiscount, '') = 'AC' THEN ISNULL(dis1.Descr, '')
--     ELSE ''
-- END,
-- [Người Liên Hệ] = cu.Attn,
-- [Số Điện Thoại] = cu.Phone