from collector.libs.pandas_utils import insert_df_to_postgres, load_df_from_postgres
from dateutil import tz

from collector.etl.abs_etl import AbstractETL
from collector.libs.logger import get_logger
from collector.libs.render_string import render_template


logger = get_logger(__name__)
local_tz = tz.tzlocal()


class TaskFetchBom(AbstractETL):
    def __init__(self, config):
        super().__init__(config)
        self.postgres_opollo_prod_conf = self.get_param_config(['postgres_opollo_prod'])
        self.query_template = self.get_param_config(["query_template"])
        self.postgres_opollo_prod_conf = self.get_param_config(["postgres_opollo_prod"])
        self.postgres_devops_warehouse_conf = self.get_param_config(["postgres_devops_warehouse"])

    @AbstractETL.wrapper_simple_log
    def execute(self, *args):
        query = render_template(
            self.query_template,
            {
                "from_time": self.from_date,
                "to_time": self.to_date
            }
        )
        # extract
        df = load_df_from_postgres(self.postgres_opollo_prod_conf, query)

        # transform
        # no action

        # load
        insert_df_to_postgres(self.postgres_devops_warehouse_conf,
                              tbl_name="product_bundles", df=df,
                              primary_keys=['id'], schema="sku")


if __name__ == '__main__':
    import datetime
    import json
    conf = {
        "app": {
            "process_type": "etl",
            "process_group": "centralize",
            "process_name": "sku",
            "process_action": "fetch_bom",
            "execution_date": datetime.datetime.now(),
            "from_date": datetime.datetime.now() - datetime.timedelta(days=1),
            "to_date": datetime.datetime.now(),
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
                "postgres_opollo_prod": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/"
                        "postgres_opollo_prod.json"
                    )
                ),
                "postgres_devops_warehouse": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/"
                        "postgres_devops_warehouse.json"
                    )
                ),
                "query_template": "select * from onpoint.product_bundles where updated_at >= '{{ from_time }}' and updated_at <= '{{ to_time }}'"
            }
        }
    }
    task_fetch_data = TaskFetchBom(conf)
    task_fetch_data.execute()
