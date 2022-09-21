from dateutil import tz
import glob
import pandas as pd
import os
import datetime

from collector.import_gdrive.abs_import_gdrive import AbstractImportBase
from collector.libs.logger import get_logger
from collector.libs.pandas_utils import insert_df_into_mongodb
from collector.libs.datetime_util import get_create_modify_time_file

logger = get_logger(__name__)
local_tz = tz.tzlocal()


class TaskFetchData(AbstractImportBase):
    def __init__(self, config):
        super().__init__(config)

    def import_local_dir(self):
        drive_out_dir = os.path.join(self.source_dir, self.folder_name) + "/*"
        in_paths = glob.glob(drive_out_dir)
        dct_dfs = dict()
        for in_path in in_paths:
            _, modify_at = get_create_modify_time_file(in_path)
            if (
                modify_at > (self.to_date + datetime.timedelta(minutes=10))
                or modify_at < self.from_date
            ):
                logger.info("skip file : {}, modify_at: {}".format(in_path, modify_at))
                continue

            file_name = os.path.basename(in_path)
            extension = os.path.splitext(in_path)[1]
            execution_date = self.execution_date
            execution_date = execution_date.astimezone(local_tz)
            if extension in (".xls", ".xlsx"):
                try:
                    dfs = pd.read_excel(in_path, sheet_name=None)
                    for sheet_name, _df in dfs.items():
                        _df["_sheet_name"] = sheet_name
                        _df["_path"] = in_path
                        _df["_file_name"] = file_name
                        _df["_log_time"] = execution_date
                    dct_dfs[file_name] = dfs
                except Exception as _:
                    self.log_error_telegram()
            elif extension == ".csv":
                try:
                    df = pd.read_csv(in_path)
                    df["_path"] = in_path
                    df["_file_name"] = file_name
                    df["_log_date"] = execution_date
                    dct_dfs[file_name] = df
                except Exception as _:
                    self.log_error_telegram()
            else:
                log_level = "WARNING"
                log_message = "{0} has extension {1} not support, please convert to (.csv, .xls, .xlsx)".format(
                    in_path, extension
                )
                self.log_telegram(log_message, log_level)

        if dct_dfs.get("KPI Master Dashboard.xlsx") is None:
            return

        dfs = dct_dfs["KPI Master Dashboard.xlsx"]
        # sheet_names = ["bp", "cf", "va", "audit"]
        dct_primary = {
            "bp": ["year", "quarter", "metric"],
            "cf": ["year", "month", "week", "metric"],
            "va": ["year", "month", "week", "type", "metric", "category"],
            "audit": ["no"],
        }
        for sheet_name in dct_primary:
            collection_name = "{}_{}".format("kpi_master", sheet_name)
            df = dfs[sheet_name]
            primary_key = dct_primary[sheet_name]
            insert_df_into_mongodb(
                self.mongo_conf, collection_name, df, primary_key=primary_key
            )

    @AbstractImportBase.wrapper_simple_log
    def execute(self, *args):
        logger.info("pull data from google drive to local")
        self.pull_gdrive()
        logger.info("import data from local to google drive")
        self.import_local_dir()
        logger.info("load data from mongo to pandas")
        from_date = self.from_date
        to_data = self.to_date

        from_date = from_date.astimezone(local_tz)
        to_data = to_data.astimezone(local_tz)
        print(from_date, to_data)


if __name__ == "__main__":
    import json

    conf = {
        "app": {
            "process_type": "import_file",
            "process_group": "marketing",
            "process_name": "lzd",
            "process_action": "fetch_data",
            "execution_date": datetime.datetime.now(),
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
                "mongo": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/mongo.json"
                    )
                ),
                "postgres": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/postgres.json"
                    )
                ),
                "gdrive": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/gdrive.json"
                    )
                ),
                "folder_id": "1m3mdtZx-QwStNBn01xJe-9r5DP3nsOnj",
                "folder_name": "kpi_master_dashboard",
            },
        }
    }
    fetch_data = TaskFetchData(config=conf)
    fetch_data.execute()
