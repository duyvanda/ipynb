from collector.etl.marketing.marketing_factory import MarketingFactory
from collector.etl.cs.cs_factory import CSFactory
from collector.etl.centralize.centralize_factory import CentralizeFactory


class ETLGroup(object):
    @staticmethod
    def get_factory(ptype):
        return {
            "marketing": MarketingFactory,
            "cs": CSFactory,
            "centralize": CentralizeFactory
        }.get(ptype)
