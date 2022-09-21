import os
import datetime
import pandas as pd
from collector.libs.logger import get_logger
from collector.libs.pandas_utils import insert_df_to_postgres, split_dataframe
from collector.libs.datetime_util import parse_time
import glob
import tqdm


logger = get_logger(__name__)


def process_new_shopee(in_file, out_dir):
    if not in_file.lower().endswith("xls"):
        logger.warning(f"{in_file} is not extension xls")
        return
    logger.info(f"Start reading {in_file}")
    df = pd.read_excel(in_file)
    if len(df.columns) != 55:
        logger.error(f"Columns reshape, please check: {in_file}")
        raise Exception(f"Columns reshape, please check: {in_file}")

    old_header = (
        "Mã đơn hàng,Forder ID,Ngày đặt hàng,Tình trạng đơn hàng,Nhận xét từ Người mua,Mã vận đơn,"
        "Lựa chọn vận chuyển,Phương thức giao hàng,Loại đơn hàng,Ngày giao hàng dự kiến,"
        "Ngày gửi hàng,Thời gian giao hàng,Tình trạng Trả hàng / Hoàn tiền,SKU sản phẩm,"
        "Tên sản phẩm,Cân nặng sản phẩm,Tổng cân nặng,Cân nặng sản phẩm.1,SKU phân loại hàng,"
        "Tên phân loại hàng,Giá gốc,Người bán tự giảm,Được Shopee trợ giá,Được người bán trợ giá,"
        "Giá ưu đãi,Số lượng,Product Subtotal,Tiền đơn hàng (VND),Mã giảm giá của Shop,Hoàn Xu,"
        "Shopee Voucher,Chỉ tiêu combo khuyến mãi,Giảm giá từ combo Shopee,"
        "Giảm giá từ Combo của Shop,Shopee Xu được hoàn,"
        "Số tiền được giảm khi thanh toán bằng thẻ Ghi nợ,"
        "Phí vận chuyển (dự kiến),Phí vận chuyển mà người mua trả,"
        "Tổng số tiền,Thời gian hoàn thành đơn hàng,"
        "Thời gian đơn hàng được thanh toán,Phương thức thanh toán,"
        "Phí cố định,Phí dịch vụ,Phí giao dịch,Tiền ký quỹ,Username (Buyer),Tên Người nhận,Số điện thoại,"
        "Tỉnh/Thành phố,TP / Quận / Huyện,District,Địa chỉ nhận hàng,Quốc gia,Ghi chú"
    )
    current_header = ",".join(df.columns.tolist())

    if current_header != old_header:
        logger.warning(f"Header changed, please check: {in_file}")
        df.rename(
            columns=dict(zip(current_header.split(","), old_header.split(","))),
            inplace=True,
        )
        # raise Exception(f'Header changed, please check: {in_file}')

    header_list = [
        "Mã đơn hàng",
        "Ngày đặt hàng",
        "Tình trạng đơn hàng",
        "Nhận xét từ Người mua",
        "Mã vận đơn",
        "Lựa chọn vận chuyển",
        "Phương thức giao hàng",
        "Loại đơn hàng",
        "Ngày giao hàng dự kiến",
        "Ngày gửi hàng",
        "Thời gian giao hàng",
        "Tình trạng Trả hàng / Hoàn tiền",
        "SKU sản phẩm",
        "Tên sản phẩm",
        "Cân nặng sản phẩm",
        "Tổng cân nặng",
        "Cân nặng sản phẩ̉m",
        "SKU phân loại hàng",
        "Tên phân loại hàng",
        "Giá gốc",
        "Người bán tự giảm",
        "Được Shopee trợ giá",
        "Được người bán trợ giá",
        "Giá ưu đãi",
        "Số lượng",
        "Product Subtotal",
        "Tiền đơn hàng (VND)",
        "Mã giảm giá của Shop",
        "Shopee Voucher",
        "Chỉ tiêu combo khuyến mãi",
        "Giảm giá từ combo Shopee",
        "Giảm giá từ Combo của Shop",
        "Shopee Xu được hoàn",
        "Số tiền được giảm khi thanh toán bằng thẻ Ghi nợ",
        "Phí vận chuyển (dự kiến)",
        "Phí vận chuyển mà người mua trả",
        "Tổng số tiền",
        "Thời gian hoàn thành đơn hàng",
        "Thời gian đơn hàng được thanh toán",
        "Phương thức thanh toán",
        "Phí cố định",
        "Phí giao dịch",
        "Tiền ký quỹ",
        "Username (Buyer)",
        "Tên Người nhận",
        "Số điện thoại",
        "Tỉnh/Thành phố",
        "TP / Quận / Huyện",
        "District",
        "Địa chỉ nhận hàng",
        "Quốc gia",
        "Ghi chú",
    ]
    set_cur_cols = set(df.columns)
    set_head_selected = set(header_list)
    logger.warning(f"diff cur cols with selected: {set_head_selected - set_cur_cols}")

    df = df.reindex(columns=header_list)
    max_row = len(df.index)

    start_time = datetime.datetime.now()
    logger.info(f"To process: {max_row} rows")

    df["SKU sản phẩm"] = df["SKU sản phẩm"].combine_first(df["SKU phân loại hàng"])

    if max_row < 1:
        logger.info(f"No data")
        return

    file_name = os.path.basename(in_file).replace(".xls", ".csv")
    df1 = df[
        (df.loc[:, "Chỉ tiêu combo khuyến mãi"] == "N")
        & (df.loc[:, "Giá ưu đãi"] != 1000)
    ]
    if len(df1.index) > 0:
        logger.info(f"Processed normal orders: {len(df1.index)}")
        for i in range(len(df1.index)):
            for j in range(1, 5):
                if i + j < len(df1.index):
                    if (
                        df1.iloc[i, 0] == df1.iloc[i + j, 0]
                        and df1.iloc[i, 12] == df1.iloc[i + j, 12]
                        and df1.iloc[i, 23] == df1.iloc[i + j, 23]
                    ):
                        df1.iloc[i + j, 0] = ""
        df1 = df1[df1.iloc[:, 0] != ""]
        df1.to_csv(
            f"{out_dir}/no_deal_{file_name}",
            index=False,
            encoding="utf-8",
            quotechar='"',
        )

    df2 = df[df.loc[:, "Giá ưu đãi"] == 1000]
    max_1k_row = len(df2.index)
    if max_1k_row < 1:
        logger.info(f"No deal 1k")
    else:
        logger.info(f"To process 1k deal orders: {max_1k_row}")
        for i in range(max_1k_row):
            for j in range(1, 10):
                if i + j < max_1k_row:
                    if (
                        df2.iloc[i, 0] == df2.iloc[i + j, 0]
                        and df2.iloc[i, 12] == df2.iloc[i + j, 12]
                        and df2.iloc[i, 23] == df2.iloc[i + j, 23]
                    ):
                        df2.iloc[i, 24] = df2.iloc[i, 24] + df2.iloc[i + j, 24]
                        df2.iloc[i, 25] = df2.iloc[i, 25] + df2.iloc[i + j, 25]
                        df2.iloc[i + j, 24] = 0
                        df2.iloc[i + j, 25] = 0
                        df2.iloc[i + j, 0] = ""
        df2 = df2[df2.iloc[:, 0] != ""]
        logger.info(f"Processed 1k deal orders: {max_1k_row}")
        df2.to_csv(
            f"{out_dir}/1k_deal_{file_name}",
            index=False,
            encoding="utf-8",
            quotechar='"',
        )
        logger.info(f"*")

    df3 = df[df.loc[:, "Chỉ tiêu combo khuyến mãi"] == "Y"]
    max_bundle_row = len(df3.index)
    if max_bundle_row < 1:
        logger.info(f"No bundle deal")
    else:
        logger.info(f"To process bundle deal orders: {max_bundle_row}")
        for i in range(max_bundle_row):
            for j in range(1, 70):
                if i + j < max_bundle_row:
                    if (
                        df3.iloc[i, 0] == df3.iloc[i + j, 0]
                        and df3.iloc[i, 12] == df3.iloc[i + j, 12]
                        and df3.iloc[i, 23] == df3.iloc[i + j, 23]
                    ):

                        df3.iloc[i, 24] = df3.iloc[i, 24] + df3.iloc[i + j, 24]
                        df3.iloc[i, 25] = df3.iloc[i, 25] + df3.iloc[i + j, 25]
                        df3.iloc[i + j, 24] = 0
                        df3.iloc[i + j, 25] = 0
                        df3.iloc[i + j, 0] = ""
        df3 = df3[df3.iloc[:, 0] != ""]
        logger.info(f"Processed bundle deal orders: {max_bundle_row}")
        df3.to_csv(
            f"{out_dir}/bundle_deal_{file_name}",
            index=False,
            encoding="utf-8",
            quotechar='"',
        )
        logger.info(f"*")

    end_time = datetime.datetime.now()
    processing_time = end_time - start_time

    logger.info(f"Processing time: {processing_time}")
    logger.info(f">>> Completed reading {file_name}")


def process_new_shopee_dir(in_dir, out_dir):
    in_path = os.path.join(in_dir, "*.xls")
    for in_file in glob.glob(in_path):
        process_new_shopee(in_file, out_dir)


def norm_value(row):
    row.insert(26, None)
    _id = row[0]
    order_created_at = row[1]
    original_status = row[2]
    user_comment = row[3].replace("\n", "") if not pd.isnull(row[3]) else None
    tracking_code = row[4].replace(".0", "") if not pd.isnull(row[4]) else None
    last_mile = row[5]
    pickup_type = row[6]
    order_type = row[7]
    delivery_date_expected = row[8]
    pickup_date_selected = row[9]
    delivery_date_actual = row[10]
    return_status = row[11]

    item_sku = (
        str(row[12]).replace(" ", "").replace("\n", "").replace(".0", "")
        if not pd.isnull(row[12])
        else None
    )
    item_name = (
        str(row[13]).replace("'", "").replace(",", "").replace("\n", "")
        if not pd.isnull(row[13])
        else None
    )

    variation_sku = (
        str(row[17]).replace(" ", "").replace("\n", "").replace(".0", "")
        if not pd.isnull(row[17])
        else None
    )
    variation_name = (
        str(row[18]).replace("'", "").replace(",", "").replace("\n", "")
        if not pd.isnull(row[18])
        else None
    )

    if row[12]:
        sku = item_sku
    else:
        sku = "BLANK"

    sku_name = (
        item_name + ": " + variation_name if not pd.isnull(row[18]) else item_name
    )
    sku_name = sku_name.replace("': '", ": ")

    sku_total_weight = row[14]
    order_total_weight = row[15]
    rrp = row[19]
    seller_discount = row[20]
    shopee_rebate = row[21]
    seller_rebate = row[22]
    selling_price = row[23]
    quantity = row[24]
    product_subtotal = row[25]
    order_items = row[26]
    order_value = row[27]
    seller_voucher = row[28]
    shopee_voucher = row[29]
    bundle_deal_indicator = row[30]
    bundle_deal_discount_shopee = row[31]
    bundle_deal_discount_seller = row[32]
    shopee_coin = row[33]
    credit_cart_discount = row[34]
    estimated_shipping_fee = row[35]
    buyer_paid_shipping_fee = row[36]
    order_grand_total = row[37]
    order_completed_at = row[38]
    order_paid_at = row[39] if not pd.isnull(row[39]) else None
    payment_type = row[40]
    fbs_fees = row[41]
    transaction_fees = row[42]
    deposit_fees = row[43]
    customer_id = row[44]
    customer_name = (
        row[45].replace("'", "").replace(",", "").replace("\n", "")
        if not pd.isnull(row[45])
        else None
    )
    customer_phone = (
        str(row[46])
        .replace("84", "0", 1)
        .replace(" ", "")
        .replace("-", "")
        .replace(".", "")
        if not pd.isnull(row[46])
        else None
    )
    province = (
        row[47].replace("'", "").replace(",", "").replace("\n", "")
        if not pd.isnull(row[47])
        else None
    )
    district = (
        row[48].replace("'", "").replace(",", "").replace("\n", "")
        if not pd.isnull(row[48])
        else None
    )
    ward = (
        row[49].replace("'", "").replace(",", "").replace("\n", "")
        if not pd.isnull(row[49])
        else None
    )
    customer_address = (
        row[50].replace("'", "").replace(",", "_").replace("\n", "")
        if not pd.isnull(row[50])
        else None
    )
    country = row[51]
    note = row[52]

    return {
        "id": _id,
        "order_created_at": order_created_at,
        "original_status": original_status,
        "user_comment": user_comment,
        "tracking_code": tracking_code,
        "last_mile": last_mile,
        "pickup_type": pickup_type,
        "order_type": order_type,
        "delivery_date_expected": delivery_date_expected,
        "pickup_date_selected": pickup_date_selected,
        "delivery_date_actual": delivery_date_actual,
        "return_status": return_status,
        "item_sku": item_sku,
        "item_name": item_name,
        "variation_sku": variation_sku,
        "variation_name": variation_name,
        "sku": sku,
        "sku_name": sku_name,
        "sku_total_weight": sku_total_weight,
        "order_total_weight": order_total_weight,
        "rrp": rrp,
        "seller_discount": seller_discount,
        "shopee_rebate": shopee_rebate,
        "seller_rebate": seller_rebate,
        "selling_price": selling_price,
        "quantity": quantity,
        "product_subtotal": product_subtotal,
        "order_items": order_items,
        "order_value": order_value,
        "seller_voucher": seller_voucher,
        "shopee_voucher": shopee_voucher,
        "bundle_deal_indicator": bundle_deal_indicator,
        "bundle_deal_discount_shopee": bundle_deal_discount_shopee,
        "bundle_deal_discount_seller": bundle_deal_discount_seller,
        "shopee_coin": shopee_coin,
        "credit_cart_discount": credit_cart_discount,
        "estimated_shipping_fee": estimated_shipping_fee,
        "buyer_paid_shipping_fee": buyer_paid_shipping_fee,
        "order_grand_total": order_grand_total,
        "order_completed_at": order_completed_at,
        "order_paid_at": order_paid_at,
        "payment_type": payment_type,
        "fbs_fees": fbs_fees,
        "transaction_fees": transaction_fees,
        "deposit_fees": deposit_fees,
        "customer_id": customer_id,
        "customer_name": customer_name,
        "customer_phone": customer_phone,
        "province": province,
        "district": district,
        "ward": ward,
        "address": customer_address,
        "country": country,
        "note": note,
    }


def import_new_shopee_dir(in_dir, postgres_conf):
    in_files = glob.glob("{}/*.csv".format(in_dir))
    lst_dbs = list()
    for in_file in in_files:
        df = pd.read_csv(in_file)
        lst_dbs.append(df)

    if len(lst_dbs) == 0:
        exit(0)
    dd = pd.concat(lst_dbs)
    lst = list()
    for v in dd.values.tolist():
        try:
            lst.append(norm_value(v))
        except Exception as _:
            print(v)
            continue

    df_norm = pd.DataFrame(lst).drop_duplicates(
        ["id", "sku", "selling_price"], keep="last"
    )
    df_norm["order_created_at"] = df_norm["order_created_at"].map(parse_time)
    df_norm["delivery_date_expected"] = df_norm["delivery_date_expected"].map(
        lambda x: parse_time(x) if not pd.isna(x) else None
    )
    df_norm["pickup_date_selected"] = df_norm["pickup_date_selected"].map(
        lambda x: parse_time(x) if not pd.isna(x) else None
    )
    df_norm["delivery_date_actual"] = df_norm["delivery_date_actual"].map(
        lambda x: parse_time(x) if not pd.isna(x) else None
    )
    df_norm["order_completed_at"] = df_norm["order_completed_at"].map(
        lambda x: parse_time(x) if not pd.isna(x) else None
    )
    df_norm["order_paid_at"] = df_norm["order_paid_at"].map(
        lambda x: parse_time(x) if not pd.isna(x) else None
    )

    chunk_size = 500
    num_total = len(df_norm) // chunk_size + 1
    with tqdm.tqdm(total=num_total) as pbar:
        for _df in split_dataframe(df_norm, chunk_size):
            _df = _df.dropna(subset=['sku'])
            insert_df_to_postgres(
                postgres_conf,
                df=_df,
                tbl_name="main_shopee",
                primary_keys=["id", "sku", "selling_price"],
            )
            pbar.update(1)


if __name__ == "__main__":
    import json

    i_dir = "/Users/thucpk/IdeaProjects/data-warehouse/data/source/bundle_deal_raw/in"
    o_dir = "/Users/thucpk/IdeaProjects/data-warehouse/data/source/bundle_deal_raw/out"
    postgres_config = json.load(
        open(
            "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/postgres_devops_warehouse.json"
        )
    )
    process_new_shopee_dir(i_dir, o_dir)
    import_new_shopee_dir(o_dir, postgres_config)
