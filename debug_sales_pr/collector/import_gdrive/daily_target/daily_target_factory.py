from collector.import_gdrive.daily_target.task_fetch_data import TaskFetchData


class DailyTargetFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {"fetch_data": TaskFetchData,}.get(process_action)(config)
