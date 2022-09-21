from collector.backup_restore.postgres.task_backup import TaskBackup
from collector.backup_restore.postgres.task_restore import TaskRestore


class PostgresFactory(object):
    @staticmethod
    def get_task(process_action, config):
        return {"backup": TaskBackup, "restore": TaskRestore}.get(process_action)(
            config
        )
