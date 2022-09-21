import psycopg2.extras
from dateutil import tz

from collector.etl.abs_etl import AbstractETL
from collector.libs.logger import get_logger
from collector.libs.postgres_utils import get_postgres_cli
from collector.libs.pandas_utils import load_df_from_postgres


logger = get_logger(__name__)
local_tz = tz.tzlocal()


class TaskCallProducer(AbstractETL):
    def __init__(self, config):
        super().__init__(config)
        self.postgres_conf = self.get_param_config(['postgres_devops_warehouse'])

    @AbstractETL.wrapper_simple_log
    def execute(self, *args):
        with get_postgres_cli(self.postgres_conf) as ps_cli:
            with ps_cli.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                df_mkt_conf = load_df_from_postgres(self.postgres_conf, "select * from marketing.mkt_conf")
                for _, row in df_mkt_conf.iterrows():
                    schema = row['schema']
                    function = row['function']
                    exec_func = "call {}.{}".format(schema, function)
                    cursor.execute(exec_func)


if __name__ == '__main__':
    import datetime
    import json

    conf = {
        "app": {
            "process_type": "etl",
            "process_group": "marketing",
            "process_name": "onpoint",
            "process_action": "fetch_data",
            "execution_date": datetime.datetime.now(),
            "from_date": datetime.datetime.now() - datetime.timedelta(days=2),
            "to_date": datetime.datetime.now() - datetime.timedelta(days=1),
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
                "zendesk": json.load(
                    open("/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/zendesk.json")
                ),
                "postgres_devops_warehouse": json.load(
                    open("/Users/thucpk/IdeaProjects/data-warehouse/data-collector/"
                         "config/default/postgres_devops_warehouse.json")
                )
            },
        }
    }
    fetch_data = TaskCallProducer(config=conf)
    fetch_data.execute()
