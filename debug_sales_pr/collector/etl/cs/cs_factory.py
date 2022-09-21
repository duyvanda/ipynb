from collector.etl.cs.task_fetch_data import TaskFetchData


class CSFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {
            "fetch_data": TaskFetchData
        }.get(process_action)(config)
