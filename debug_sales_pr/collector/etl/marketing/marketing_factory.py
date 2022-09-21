from collector.etl.marketing.task_mapping_centralize import TaskMappingCentralize
from collector.etl.marketing.task_call_producer import TaskCallProducer


class MarketingFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {
            "mapping_centralize": TaskMappingCentralize,
            "call_producer": TaskCallProducer
        }.get(process_action)(config)
