from collector.etl.centralize.task_fetch_bom import TaskFetchBom


class CentralizeFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {
            "fetch_bom": TaskFetchBom
        }.get(process_action)(config)
