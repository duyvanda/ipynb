from dateutil import tz
import glob
import pandas as pd
import os
import datetime
import calendar
import itertools
import tqdm

from collector.import_gdrive.abs_import_gdrive import AbstractImportBase
from collector.libs.logger import get_logger
from collector.libs.pandas_utils import insert_df_to_postgres


logger = get_logger(__name__)
local_tz = tz.tzlocal()


def convert_int(x, default=0):
    try:
        return int(float(x))
    except Exception as _:
        return default


class TaskFetchData(AbstractImportBase):

    def __init__(self, config):
        super().__init__(config)
        self.postgres_conf = self.get_param_config(["postgres"])
        self.source_dir = self.get_param_config(["source_dir"])
        self.folder_name = self.get_param_config(["folder_name"])
        self.file_name = self.get_param_config(['file_name'])

    def extract_raw(self):
        drive_out_dir = os.path.join(self.source_dir, self.folder_name) + "/*"
        in_path = glob.glob(drive_out_dir)[0]
        df_raw = pd.read_excel(in_path, sheet_name='capture1654').dropna(subset=['Brand', 'Platform'])
        return df_raw

    @staticmethod
    def transform(df_raw):
        cols = df_raw.columns.tolist()
        # fix temp
        # df_raw.loc[(df_raw['Group Brand'] == 'Dist. FMCG 4') & (df_raw['Brand'] == 'KAO'), 'Brand'] = 'Dist. FMCG 4'
        ix_order = cols.index('Order')
        ix_nmv = cols.index('NMV Daily')
        ix_calendar = cols.index('CP Calendar')
        ix_traffic = cols.index('Traffic')
        # ix_fulfillment = cols.index('Fulfillment Model')
        index_first_day = cols.index('NMV Daily') + 2
        first_day = cols[index_first_day]
        num_days = calendar.monthrange(first_day.year, first_day.month)[1]

        df_etl = pd.DataFrame()
        df_etl['brand'] = df_raw.loc[:, 'Brand']
        df_etl['platform'] = df_raw.loc[:, 'Platform']
        df_etl = df_etl[['brand', 'platform']].dropna()
        df_etl['brand_platform'] = df_etl['brand'] + df_etl['platform']
        brand_platform = df_etl['brand_platform'].tolist()
        str_dates = pd.date_range(
            first_day, first_day+datetime.timedelta(num_days-1)).map(lambda x: str(x.date())).tolist()
        df_product = pd.DataFrame(list(itertools.product(df_etl['brand_platform'], str_dates)),
                                  columns=['brand_platform', 'date'])
        df_final = df_etl.join(df_product.set_index('brand_platform'), ['brand_platform']).reset_index(drop=True)

        # order
        df_order = df_raw.iloc[:, ix_order+2:(ix_order+2+num_days)]
        df_order.columns = str_dates
        df_order_t = df_order.T
        df_order_t.columns = brand_platform

        # nmv
        df_nmv = df_raw.iloc[:, ix_nmv+2:(ix_nmv+2+num_days)]
        df_nmv.columns = str_dates
        df_nmv_t = df_nmv.T
        df_nmv_t.columns = brand_platform

        # calendar
        df_calendar = df_raw.iloc[:, ix_calendar+1:ix_calendar+1+num_days]
        df_calendar.columns = str_dates
        df_calendar_t = df_calendar.T
        df_calendar_t.columns = brand_platform

        # traffic
        df_traffic = df_raw.iloc[:, ix_traffic+2:ix_traffic+2+num_days]
        df_traffic.columns = str_dates
        df_traffic_t = df_traffic.T
        df_traffic_t.columns = brand_platform

        # fulfillment
        # df_fulfillment = df_raw.iloc[:, ix_fulfillment+1:ix_fulfillment+1+num_days]
        # df_fulfillment.columns = str_dates
        # df_fulfillment_t = df_fulfillment.T
        # df_fulfillment_t.columns = brand_platform

        df_final['order_target'] = None
        df_final['nmv_target'] = None
        df_final['day_type'] = None
        df_final['traffic_target'] = None
        df_final['fulfillment_model'] = None

        with tqdm.tqdm(total=df_final.shape[0]) as pbar:
            for _, r in df_final.iterrows():
                bp = r['brand_platform']
                d = r['date']
                try:
                    r['order_target'] = df_order_t[bp][d]
                except Exception as e:
                    print(bp)
                    print(d)
                    print(df_order_t[bp])
                    raise e
                r['nmv_target'] = df_nmv_t[bp][d]
                r['day_type'] = df_calendar_t[bp][d]
                r['traffic_target'] = df_traffic_t[bp][d]
                # r['fulfillment_model'] = df_fulfillment_t[bp][d]
                pbar.update(1)

        df_final['day_type'] = df_final['day_type'].fillna('NOTARGET')
        df_final['order_target'] = df_final['order_target'].fillna('').map(convert_int).fillna(0)
        df_final['nmv_target'] = df_final['nmv_target'].fillna('').map(convert_int).fillna(0)
        df_final['traffic_target'] = df_final['traffic_target'].fillna('').map(convert_int).fillna(0)
        # df_final["date"] = df_final["date"]
        df_final = df_final.dropna(subset=["brand", "platform", "date", "day_type"])
        return df_final

    def load_data(self, df):
        # print(df['date'].min(), df['date'].max())
        insert_df_to_postgres(self.postgres_conf, tbl_name="calendar",
                              df=df, primary_keys=["brand", "platform", "date", "day_type"])

    @AbstractImportBase.wrapper_simple_log
    def execute(self, *args):
        logger.info("step 0: pull data from google drive to local")
        str_query = "(title = '{}')".format(self.file_name)
        drive_out_dir = os.path.join(self.source_dir, self.folder_name)
        self.pull_gdrive(str_query, drive_out_dir)
        logger.info("step 1: extract raw data")
        df_raw = self.extract_raw()
        # df_raw = df_raw.loc[df_raw['Brand'] != '#REF!']
        logger.info("df_raw: {}".format(len(df_raw)))
        logger.info("step 2: transform data")
        df_norm = self.transform(df_raw)
        logger.info("step 3: load data to database")
        print(df_norm['nmv_target'].map(float).sum())
        print(df_norm['traffic_target'].map(float).sum())
        print(df_norm['order_target'].map(float).sum())
        print(df_norm.groupby(['brand'])['nmv_target'].sum())
        self.load_data(df_norm)


if __name__ == "__main__":
    import json

    conf = {
        "app": {
            "process_type": "import_gdrive",
            "process_group": "haravan",
            "process_name": "lzd",
            "process_action": "fetch_data",
            "execution_date": datetime.datetime.now(),
            "from_date": datetime.datetime.now() - datetime.timedelta(10),
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
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/"
                        "postgres_devops_warehouse.json"
                    )
                ),
                "gdrive": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/gdrive_local.json"
                    )
                ),
                "file_name": "Target data for BI",
                "folder_name": "target_data",
                "pattern_match": "Target data for BI.xlsx"
            },
        }
    }
    fetch_data = TaskFetchData(config=conf)
    fetch_data.execute()
