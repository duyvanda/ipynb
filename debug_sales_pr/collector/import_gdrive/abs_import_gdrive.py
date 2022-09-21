import datetime
from abc import ABC
from dateutil import tz

from collector.core.abstract_base import AbstractBase
from collector.libs.logger import get_logger
from collector.gsuide_libs.gdrive_utils import GDriveUtils


logger = get_logger(__name__)

utc_zone = tz.tzutc()
local_tz = tz.tzlocal()


class AbstractImportBase(AbstractBase, ABC):
    def __init__(self, config):
        super(AbstractImportBase, self).__init__(config)
        self.process_action: str = self.get_process_info(["process_action"])
        self.from_date: datetime.datetime = self.get_process_info(["from_date"])
        self.to_date: datetime.datetime = self.get_process_info(["to_date"])
        gdrive_conf = self.get_param_config(["gdrive"])
        self.gdrive_utils = GDriveUtils(gdrive_conf)
        # self.source_dir = self.get_param_config(["source_dir"])

    def pull_gdrive(self, str_query, drive_out_dir):
        str_from_date_utc = self.from_date.astimezone(utc_zone).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        str_to_date_utc = self.to_date.astimezone(utc_zone).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        gdrive_query = {
            "q": "modifiedDate >= '{0}' AND modifiedDate < '{1}' AND {2} AND trashed=false".format(
                str_from_date_utc, str_to_date_utc, str_query
            )
        }
        logger.info("gdrive query {}".format(gdrive_query))
        dct_folder_abs_path = {}
        self.gdrive_utils.down_load_files_match(
            gdrive_query, dct_folder_abs_path, drive_out_dir
        )
