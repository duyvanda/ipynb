from dateutil import tz
import glob
import pandas as pd
import os
import datetime
import re
import tqdm

from collector.import_gdrive.abs_import_gdrive import AbstractImportBase
from collector.libs.logger import get_logger
from collector.libs.pandas_utils import insert_df_to_postgres
from collector.libs.datetime_util import parse_time


logger = get_logger(__name__)
local_tz = tz.tzlocal()


class TaskFetchData(AbstractImportBase):
    mapping_v1 = {
        'Id': 'id',
        'Email': 'customer_email',
        'Doanh Số': 'order_value',
        'Phí vận chuyển': 'shipping_fee',
        'Ngày đặt hàng': 'order_created_at',
        'Thời gian đặt hàng': 'order_created_at_time',
        'Số lượng sản phẩm': 'quantity',
        'Tên sản phẩm': 'sku_name',
        'Giá sản phẩm': 'selling_price',
        'Giá niêm yết': 'rsp',
        'Barcode': 'sku',
        'Tên người nhận': 'customer_name',
        'Địa chỉ nhận hàng': 'customer_address',
        'Quận/Huyện nhận hàng': 'district',
        'Tỉnh/TP nhận hàng': 'province',
        'Số điện thoại': 'customer_phone',
        'Phương thức thanh toán': 'payment_type',
        'Thương hiệu': 'brand',
        'Chi nhánh': 'branch',
        'Trạng thái POS': 'original_status',
        'shop_account': 'shop_account'
    }

    mapping_status_v1 = {
        "Hoàn tất": "Completed",
        "Hủy đơn": "Canceled",
        "NVC giao hàng": "Shipping",
        "Hủy đơn - Nhập kho": "Returned",
        "Đã xác nhận": "Processing",
        "Chờ xử lý": "Processing",
        "Đã gán user": "Processing",
        "Đã xuất kho": "Processing",
        "Hủy trả hàng": "Completed"
    }

    mapping_v2 = {
        'Id': 'id',
        'Ngày đặt hàng': 'order_created_at',
        'Kho bán': 'branch',
        'Email': 'customer_email',
        'Tên người nhận': 'customer_name',
        'Số điện thoại': 'customer_phone',
        'Shipping Street': 'customer_address',
        'Quận/Huyện nhận hàng': 'district',
        'Tỉnh/TP nhận hàng': 'province',
        'Mã sản phẩm': 'sku',
        'Tên sản phẩm': 'sku_name',
        'Số lượng sản phẩm': 'quantity',
        'Giá sản phẩm': 'selling_price',
        'Tổng tiền': 'order_value',
        'Hãng': 'brand',
        'Mã khuyến mãi': 'voucher_code',
        'Số tiền giảm': 'voucher',
        'Phí vận chuyển': 'shipping_fee',
        'Phương thức thanh toán': 'payment_type',
        'Tình trạng giao hàng': 'shipping_status',
        #  'Ngày hủy': 'cancel_date',
        'Trạng thái hủy': 'cancel_status',
        'shop_account': 'shop_account'
        #  'Nhóm lý do hủy': 'cancel_reason',
        #  'Đơn vị giao hàng': 'last_mile',
        #  'Mã vận đơn': 'tracking_code'
    }

    mapping_brand = {
        "La Roche Posay": "LA ROCHE-POSAY",
        "KIEHL'S": "KIEHLS",
        'Vichy': "VICHY",
        "L'Oréal Paris": "LOREAL PARIS",
        'shu uemura': "SHU",
        'LANCÔME': "LANCOME",
        'Maybelline New York': "MAYBELLINE",
        'L’ORÉAL PROFESSIONNEL': "PPD",
        'YSL Beauty': "YSL",
        'WHITE PERFECT': "LOREAL PARIS",
        'UV PERFECT': "LOREAL PARIS",
        'MICELLAR': "LOREAL PARIS",
        'Lancome': "LANCOME",
        'REVITALIFT': "LOREAL PARIS",
        'MICRO ESSENCE': "LOREAL PARIS",
        'EXCELLENCE': "LOREAL PARIS",
        'ROUGE SIGNATURE': "LOREAL PARIS",
        'YOUTH CODE': "LOREAL PARIS",
        'INFALLIBLE': "LOREAL PARIS",
        'COLOR RICHE': "LOREAL PARIS",
        'TRUE MATCH': "LOREAL PARIS",
        'MASCARA': "LOREAL PARIS",
        'BROW ARTIST': "LOREAL PARIS"
    }

    @staticmethod
    def get_status_v2(x):
        is_cancel = x['cancel_status']
        is_shipping = x['shipping_status']
        if is_cancel.lower() == 'yes':
            if is_shipping.lower() == 'chưa hoàn thành':
                return 'Canceled'
            else:
                return 'Returned'
        else:
            if is_shipping.lower() == 'đã hoàn thành':
                return 'Completed'
            else:
                return 'Processing'

    def __init__(self, config):
        super().__init__(config)
        self.source_dir = self.get_param_config(["source_dir"])
        self.postgres_conf = self.get_param_config(["postgres"])
        self.pattern_match = self.get_param_config(['pattern_match'])
        self.folder_id = self.get_param_config(['folder_id'])
        self.folder_name = self.get_param_config(["folder_name"])

    def extract_raw(self):
        drive_out_dir = os.path.join(self.source_dir, self.folder_name) + "/*"
        all_paths = glob.glob(drive_out_dir)
        pattern_match = re.compile(self.execution_date.strftime(self.pattern_match))
        in_paths = list()
        # print(pattern_match)
        for in_path in all_paths:
            if pattern_match.search(in_path):
                in_paths.append(in_path)

        dfs_v1 = list()
        dfs_v2 = list()
        with tqdm.tqdm(total=len(in_paths)) as pbar:
            for in_path in in_paths:
                df = pd.read_excel(in_path, dtype=str)
                df['shop_account'] = os.path.basename(in_path).split('.')[0]
                # df_v1 shape[1] in (64, 66)
                dfs_v1.append(df)
                # turn off version 2
                # self.log_telegram(
                #     "please check data {}, shape {} not correct for parse".format(in_path, df.shape),
                #     log_level="WARNING")
                pbar.update(1)
        return dfs_v1, dfs_v2

    def transform(self, dfs_v1, dfs_v2):
        df_haravan_v1 = pd.DataFrame()
        if len(dfs_v1) > 0:
            df_raw_v1 = pd.concat(dfs_v1, ignore_index=True)
            df_haravan_v1 = df_raw_v1[self.mapping_v1.keys()].rename(columns=self.mapping_v1)
            df_haravan_v1['order_created_at'] = (df_haravan_v1['order_created_at'] + "T" +
                                                 df_haravan_v1['order_created_at_time']
                                                 ).map(lambda x: parse_time(x, '%d-%m-%YT%H:%M'))
            df_haravan_v1 = df_haravan_v1.drop(columns=['order_created_at_time'])
            df_haravan_v1 = df_haravan_v1.loc[~df_haravan_v1['sku'].isna()]
            df_haravan_v1['status'] = df_haravan_v1['original_status'].map(
                lambda x: self.mapping_status_v1.get(x, "Processing"))

        df_haravan_v2 = pd.DataFrame()
        if len(dfs_v2) > 0:
            df_raw_v2 = pd.concat(dfs_v2, ignore_index=True)
            df_haravan_v2 = df_raw_v2[self.mapping_v2.keys()].rename(columns=self.mapping_v2)
            df_haravan_v2['order_created_at'] = df_haravan_v2['order_created_at'].map(parse_time)
            df_haravan_v2['status'] = df_haravan_v2.apply(self.get_status_v2, axis=1)
            df_haravan_v2 = df_haravan_v2.drop(columns=['shipping_status', 'cancel_status'])

        df_haravan = pd.concat([df_haravan_v1, df_haravan_v2])
        if df_haravan.shape[0] > 0:
            num_raw = df_haravan.shape[0]
            df_haravan['platform'] = 'BRAND.COM'
            df_haravan = df_haravan.drop_duplicates(['platform', 'sku', 'id'])
            df_haravan['brand'] = df_haravan['brand'].map(lambda x: self.mapping_brand.get(x, x))
            df_haravan['customer_id'] = df_haravan['customer_phone']
            df_haravan['selling_price'] = df_haravan['selling_price'].map(lambda x: int(float(x)))
            df_haravan.dropna(subset=['sku'], inplace=True)
            df_haravan['order_returned'] = df_haravan['status'].map(lambda x: 1 if x == 'Returned' else 0)
            df_haravan.loc[df_haravan['brand'].isna(), 'brand'] = df_haravan.loc[
                df_haravan['brand'].isna(), 'shop_account'].str.upper()
            df_haravan.loc[df_haravan['original_status'].isna(), 'original_status'] = df_haravan.loc[
                df_haravan['original_status'].isna(), 'status']
            df_haravan = df_haravan.drop_duplicates(
                ['platform', 'sku', 'id', 'selling_price', 'order_returned']
            )
            df_haravan['rsp'] = df_haravan['rsp'].map(lambda x: float(x))
            df_haravan.loc[df_haravan['rsp'].isna(), 'rsp'] = df_haravan.loc[df_haravan['rsp'].isna(), 'selling_price']
            df_haravan['rsp'] = df_haravan.apply(lambda x: x['selling_price'] if x['rsp'] == 0 else x['rsp'], axis=1)
            df_haravan['rsp'] = df_haravan['rsp'].map(int)

            num_norm = df_haravan.shape[0]
            drop_percent = (num_raw - num_norm) / num_raw
            if drop_percent > 0.1:
                self.log_telegram("haravan num raw: {}, drop_percent: {} > 10 percent".format(
                    num_raw, drop_percent * 100), "WARNING")
            return df_haravan
        else:
            raise Exception("haravan empty")

    def load_data(self, df_haravan):
        insert_df_to_postgres(
            postgres_conf=self.postgres_conf,
            tbl_name='main_order2019',
            df=df_haravan,
            primary_keys=['platform', 'sku', 'id', 'selling_price', 'order_returned']
        )

    @AbstractImportBase.wrapper_simple_log
    def execute(self, *args):
        logger.info("step 0: pull data from google drive to local")
        str_query = "('{}' in parents)".format(self.folder_id)
        drive_out_dir = os.path.join(self.source_dir, self.folder_name)
        self.pull_gdrive(str_query, drive_out_dir)
        logger.info("step 1: extract raw data")
        df_raw_v1, df_raw_v2 = self.extract_raw()
        logger.info("df_raw_v1: {}, df_raw_v2: {}".format(len(df_raw_v1), len(df_raw_v2)))
        logger.info("step 2: transform data")
        df_norm = self.transform(df_raw_v1, df_raw_v2)
        logger.info("step 3: load data to database")
        self.load_data(df_norm)


if __name__ == "__main__":
    import json

    conf = {
        "app": {
            "process_type": "import_gdrive",
            "process_group": "haravan",
            "process_name": "lzd",
            "process_action": "fetch_data",
            "execution_date": datetime.datetime.now() - datetime.timedelta(1),
            "from_date": datetime.datetime.now() - datetime.timedelta(1),
            "to_date": datetime.datetime.now(),
            "params": {
                "telegram": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/telegram.json"
                    )
                ),
                "telegram_users": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/telegram_users.json"
                    )
                ),
                "source_dir": "/Users/thucpk/IdeaProjects/data-warehouse/data/source",
                "archive_dir": "/Users/thucpk/IdeaProjects/data-warehouse/data/archive",
                "postgres": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/postgres_prod.json"
                    )
                ),
                "gdrive": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/gdrive_local.json"
                    )
                ),
                "folder_id": "17pRVKwtLDs6kuRqa5q6bDseFaLncTjdF",
                "folder_name": "BRAND.COM/daily",
                "pattern_match": ".*_%d%m.*xls"
            },
        }
    }
    fetch_data = TaskFetchData(config=conf)
    fetch_data.execute()
