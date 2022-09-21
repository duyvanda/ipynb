from collector.import_gdrive.kpi_master.kpi_master_factory import KPIMasterFactory
from collector.import_gdrive.haravan.haravan_factory import HaravanrFactory


class ImportGDriveGroup(object):
    @staticmethod
    def get_factory(ptype):
        return {
            "kpi_master": KPIMasterFactory,
            "haravan": HaravanrFactory
        }.get(ptype)
