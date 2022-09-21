from abc import ABC
from dateutil import tz

from collector.core.abstract_base import AbstractBase
from collector.libs.logger import get_logger

logger = get_logger(__name__)

utc_zone = tz.tzutc()


class AbstractBackupRestore(AbstractBase, ABC):
    def __init__(self, config):
        super(AbstractBackupRestore, self).__init__(config)
        self.archive_dir = self.get_param_config(["archive_dir"])
