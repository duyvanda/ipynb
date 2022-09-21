from collector.libs.pandas_utils import insert_df_to_postgres, load_df_from_postgres
from collector.libs.postgres_utils import get_list_primary
import pandas as pd
from dateutil import tz

from collector.etl.abs_etl import AbstractETL
from collector.libs.logger import get_logger


logger = get_logger(__name__)
local_tz = tz.tzlocal()


class TaskMappingCentralize(AbstractETL):
    def __init__(self, config):
        super().__init__(config)
        self.postgres_conf = self.get_param_config(['postgres_devops_warehouse'])
        self.df_mkt_mapping = load_df_from_postgres(self.postgres_conf, query="""
        select * from "marketing"."mkt_mapping"
        """)

    @staticmethod
    def get_platform(src_table):
        tbl_name = src_table.upper()
        if 'SHOPEE' in tbl_name:
            return 'SHOPEE'
        elif ('LAZADA' in tbl_name) or ('LZD' in tbl_name):
            return 'LZD'
        elif 'TIKI' in tbl_name:
            return 'TIKI'
        elif 'SENDO' in tbl_name:
            return 'SENDO'
        else:
            return 'NOT_FOUND'

    @AbstractETL.wrapper_simple_log
    def execute(self, *args):
        src_tables = set(self.df_mkt_mapping['source_table'])
        dsc_tables = set(self.df_mkt_mapping['destination_table'])
        dct_dfs = dict()
        for src_tbl in src_tables:
            for dsc_tbl in dsc_tables:
                df_mapping_info = self.df_mkt_mapping.loc[
                    (self.df_mkt_mapping['source_table'] == src_tbl) &
                    (self.df_mkt_mapping['destination_table'] == dsc_tbl)]

                if df_mapping_info.shape[0] > 0:
                    src_cols = df_mapping_info['source_column']
                    dsc_cols = df_mapping_info['destination_column']
                    alias = zip(src_cols, dsc_cols)
                    selected_cols = ", ".join(map(lambda x: '"{0}" as "{1}"'.format(x[0], x[1]), alias))
                    query = "select {} from {} where monitor_time >= '{}' AND monitor_time < '{}'".format(
                        selected_cols, src_tbl, self.from_date, self.to_date)
                    df = load_df_from_postgres(postgres_conf=self.postgres_conf, query=query)
                    if df.shape[0] > 0:
                        df['platform'] = self.get_platform(src_tbl)
                        dct_dfs["{}:::{}".format(src_tbl, dsc_tbl)] = df

        if len(dct_dfs) > 0:
            for dsc_tbl in dsc_tables:
                filter_condition = ":::{}".format(dsc_tbl)
                dfs = [v for k, v in dct_dfs.items() if filter_condition in k]
                if len(dfs) > 0:
                    primary_keys = get_list_primary(self.postgres_conf, dsc_tbl, schema='marketing')
                    df_concat = pd.concat(dfs)
                    if len(primary_keys) > 0:
                        df_concat = df_concat.drop_duplicates(primary_keys)
                    insert_df_to_postgres(
                        postgres_conf=self.postgres_conf,
                        tbl_name=dsc_tbl,
                        df=df_concat,
                        primary_keys=primary_keys,
                        schema="marketing"
                    )
                else:
                    self.log_telegram(
                        log_message="marketing mapping centralize: destination table {0} data not found".format(
                            dsc_tbl),
                        log_level="WARNING")
        else:
            self.log_telegram(log_message="marketing mapping centralize: all data not found", log_level="WARNING")
