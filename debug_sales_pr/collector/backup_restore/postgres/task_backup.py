from collector.backup_restore.abs_backup_restore import AbstractBackupRestore
from collector.libs.logger import get_logger
from collector.libs.utils import run_command
from collector.libs.render_string import render_template
from collector.libs.postgres_utils import get_tables_postgres

logger = get_logger(__name__)


class TaskBackup(AbstractBackupRestore):
    template_dump_db = """
    export PGPASSWORD={{ postgres_password }}
    
    pg_dump -h {{ postgres_host }} \
             -p {{ postgres_port }} \
             -U {{ postgres_user }} \
             -F c -b -v \
             -f "{{ file_dump }}" {{ postgres_db }}
    echo "workdir: $(pwd)"
    mv "{{ file_dump }}" "{{archive_dir}}"
    """

    template_dump_table = """
    export PGPASSWORD={{ postgres_password }}
    
    pg_dump -h {{ postgres_host }} \
             -p {{ postgres_port }} \
             -U {{ postgres_user }} \
             -F c -b -v \
             -t {{ postgres_table }} \
             -f "{{ file_dump }}" {{ postgres_db }}
    
    mv "{{ file_dump }}" "{{archive_dir}}"
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
        self.tables: list = self.get_param_config(["tables"], False)
        if self.tables is None:
            self.tables = list()

    def snapshot_database(self):
        file_dump = "{0}_{1}".format(
            self.postgres_conf["database"], int(self.execution_date.timestamp())
        )
        params = {
            "postgres_host": self.postgres_conf["host"],
            "postgres_port": self.postgres_conf["port"],
            "postgres_user": self.postgres_conf["user"],
            "postgres_password": self.postgres_conf["password"],
            "postgres_db": self.postgres_conf["database"],
            "file_dump": file_dump,
            "archive_dir": self.archive_dir,
        }
        cmd = render_template(self.template_dump_db, params)
        logger.debug("execute with cmd: {0}".format(cmd))
        rc, stdout, stderr = run_command(cmd)
        logger.info("rc: {}".format(rc))
        logger.info("stdout: {}".format(stdout))
        logger.info("stderr: {}".format(stderr))
        logger.info("execute cmd done")

    def snapshot_table(self):
        if len(self.tables) > 0:
            tables = self.tables
        else:
            df_table = get_tables_postgres(self.postgres_conf)
            if df_table.shape[0] > 0:
                tables = df_table["table_name"]
            else:
                logger.info(
                    "not found table in database {}".format(
                        self.postgres_conf["database"]
                    )
                )
                tables = []
        for tbl in tables:
            file_dump = "{0}_{1}_{2}".format(
                self.postgres_conf["database"],
                tbl,
                int(self.execution_date.timestamp()),
            )
            params = {
                "postgres_host": self.postgres_conf["host"],
                "postgres_port": self.postgres_conf["port"],
                "postgres_user": self.postgres_conf["user"],
                "postgres_password": self.postgres_conf["password"],
                "postgres_db": self.postgres_conf["database"],
                "postgres_table": tbl,
                "file_dump": file_dump,
                "archive_dir": self.archive_dir,
            }
            cmd = render_template(self.template_dump_table, params)
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
                "archive_dir": "/Users/thucpk/IdeaProjects/data-warehouse/data/archive",
                "postgres": {
                    "host": "localhost",
                    "port": 5432,
                    "user": "airflow",
                    "password": "airflow",
                    "database": "airflow",
                },
                # json.load(
                # open(
                #     "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/postgres.json"
                # )
                # ),
                "type_data": "database",
                "action_backup": "snapshot",
                "tables": [],
            },
        }
    }
    task_backup = TaskBackup(conf)
    task_backup.execute()
