from collector.import_gdrive.kpi_master.task_fetch_data import TaskFetchData


class KPIMasterFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {"fetch_data": TaskFetchData,}.get(process_action)(config)
