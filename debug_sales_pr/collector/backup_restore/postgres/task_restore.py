from collector.backup_restore.abs_backup_restore import AbstractBackupRestore
from collector.libs.logger import get_logger
from collector.libs.utils import run_command
from collector.libs.render_string import render_template
import os

logger = get_logger(__name__)


class TaskRestore(AbstractBackupRestore):
    template_restore_db = """
    export PGPASSWORD={{ postgres_password }}
    
    pg_restore -h {{ postgres_host }} \
             -p {{ postgres_port }} \
             -U {{ postgres_user }} \
             -d {{ postgres_db }} \
             -v "{{ file_dump }}"    
    """

    template_restore_table = """
    export PGPASSWORD={{ postgres_password }}
    
    pg_restore -h {{ postgres_host }} \
             -p {{ postgres_port }} \
             -U {{ postgres_user }} \
             -d {{ postgres_db }} \
             -t {{ postgres_table }} \
             -v "{{ file_dump }}"    
    """

    def __init__(self, config):
        super().__init__(config)
        self.postgres_conf = self.get_param_config(["postgres"])
        self.action_backup = self.get_param_config(["action_backup"], False)
        self.type_data = self.get_param_config(["type_data"], False)
        if self.action_backup is None:
            self.action_backup = "snapshot"
        if self.type_data is None:
            self.type_data = "database"
        if self.type_data == "database":
            self.table: str = self.get_param_config(["table"], False)
        else:
            self.table: str = self.get_param_config(["table"])
            assert len(self.table) > 0, "please input table name"
        self.file_dump = self.get_param_config(["file_dump"])

    def snapshot_database(self):
        params = {
            "postgres_host": self.postgres_conf["host"],
            "postgres_port": self.postgres_conf["port"],
            "postgres_user": self.postgres_conf["user"],
            "postgres_password": self.postgres_conf["password"],
            "postgres_db": self.postgres_conf["database"],
            "file_dump": os.path.join(self.archive_dir, self.file_dump),
        }
        cmd = render_template(self.template_restore_db, params)
        logger.debug("execute with cmd: {0}".format(cmd))
        rc, stdout, stderr = run_command(cmd)
        logger.info("rc: {}".format(rc))
        logger.info("stdout: {}".format(stdout))
        logger.info("stderr: {}".format(stderr))
        logger.info("execute cmd done")

    def snapshot_table(self):
        params = {
            "postgres_host": self.postgres_conf["host"],
            "postgres_port": self.postgres_conf["port"],
            "postgres_user": self.postgres_conf["user"],
            "postgres_password": self.postgres_conf["password"],
            "postgres_db": self.postgres_conf["database"],
            "postgres_table": self.table,
            "file_dump": os.path.join(self.archive_dir, self.file_dump),
        }
        cmd = render_template(self.template_restore_table, params)
        logger.debug("execute with cmd: {0}".format(cmd))
        rc, stdout, stderr = run_command(cmd)
        logger.info("rc: {}".format(rc))
        logger.info("stdout: {}".format(stdout))
        logger.info("stderr: {}".format(stderr))
        logger.info("execute cmd done")

    def execute_snapshot(self):
        if self.type_data == "database":
            self.snapshot_database()
        else:
            self.snapshot_table()

    def execute(self, *args):
        if self.action_backup == "snapshot":
            self.execute_snapshot()
        else:
            self.log_telegram(
                "action backup: {} not support".format(self.action_backup)
            )


if __name__ == "__main__":
    import datetime
    import json

    conf = {
        "app": {
            "process_type": "backup_restore",
            "process_group": "postgres",
            "process_name": "airflow",
            "process_action": "backup",
            "execution_date": datetime.datetime.now(),
            "params": {
                "telegram": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/telegram.json"
                    )
                ),
                "telegram_users": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/telegram_users.json"
                    )
                ),
                "postgres": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/postgres.json"
                    )
                ),
                "archive_dir": "/Users/thucpk/IdeaProjects/data-warehouse/data/archive",
                "file_dump": "airflow_1596389134",
                "type_data": "table",
                "action_backup": "snapshot",
                "table": "log",
            },
        }
    }
    task_restore = TaskRestore(conf)
    task_restore.execute()
