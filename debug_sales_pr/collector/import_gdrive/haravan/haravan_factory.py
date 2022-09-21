from collector.import_gdrive.haravan.task_fetch_data import TaskFetchData


class HaravanrFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {"fetch_data": TaskFetchData,}.get(process_action)(config)
