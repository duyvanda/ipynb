import datetime
from abc import ABC
from collector.core.abstract_base import AbstractBase
from collector.libs.logger import get_logger


logger = get_logger(__name__)


class AbstractETL(AbstractBase, ABC):
    def __init__(self, config):
        super(AbstractETL, self).__init__(config)
        self.process_action: str = self.get_process_info(["process_action"])
        self.from_date: datetime.datetime = self.get_process_info(["from_date"])
        self.to_date: datetime.datetime = self.get_process_info(["to_date"])
