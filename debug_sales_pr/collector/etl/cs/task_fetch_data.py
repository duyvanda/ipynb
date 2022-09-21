from collector.libs.pandas_utils import insert_df_to_postgres
import pandas as pd
import requests
from dateutil import tz
import datetime

from collector.etl.abs_etl import AbstractETL
from collector.libs.logger import get_logger
from collector.libs.datetime_util import parse_time


logger = get_logger(__name__)
local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskFetchData(AbstractETL):
    def __init__(self, config):
        super().__init__(config)
        self.postgres_conf = self.get_param_config(['postgres_devops_warehouse'])
        zendesk = self.get_param_config(["zendesk"])
        credentials = tuple(zendesk['credentials'])
        self.zendesk_url = zendesk['url']
        session = requests.Session()
        session.auth = credentials
        self.session = session
        str_from = self.from_date.astimezone(
            tz.tzutc()
        ).replace(tzinfo=utc_tz, microsecond=0).isoformat()
        str_to = self.to_date.astimezone(
            tz.tzutc()
        ).replace(tzinfo=utc_tz, microsecond=0).isoformat()
        self.params = {
            'query': 'created>={0} created<={1}'.format(str_from, str_to),
            'sort_by': 'created_at',
            'sort_order': 'asc'  # 'desc'
        }

    @staticmethod
    def parse_fields(x):
        rs = dict()
        for r in x:
            rs[r['id']] = r['value']
        return rs

    @staticmethod
    def valid_datetime(x):
        try:
            if pd.isna(x):
                return None
            if (x < datetime.datetime(2018, 1, 1)) or (x > (datetime.datetime.now() + datetime.timedelta(1))):
                return None
        except Exception as e:
            print(x)
            raise e
        return x

    def extract_raw(self):
        session = self.session
        params = self.params
        raw = list()
        response = session.get(self.zendesk_url, params=params)
        if response.status_code != 200:
            print(response.text)
            print('Status:', response.status_code, 'Problem with the request. Exiting.')
        data = response.json()
        results = data['results']
        raw.extend(results)
        next_page = data['next_page']
        if next_page is not None:
            print(next_page)
            response = session.get(next_page)
        while (response.status_code == 200) and (next_page is not None):
            try:
                data = response.json()
                results = data['results']
                raw.extend(results)
                next_page = data['next_page']
                response = session.get(next_page)
            except Exception as e:
                print(e, response)

        print(response.text)
        print('Status:', response.status_code, 'Problem with the request. Exiting.')
        df_raw = pd.DataFrame(raw)
        if df_raw.shape[0] == 0:
            exit(0)
        return df_raw

    def transform(self, df_raw):
        selected = ["id", "created_at", "updated_at", "subject", "status", 'description',
                    "requester_id", "assignee_id", "organization_id",
                    "group_id", "fields", "brand_id"]
        df_cs = df_raw[selected]
        # df_cs['fields'] = df_cs['fields'].map(lambda x: ast.literal_eval(x))

        df_fields = pd.DataFrame(df_cs['fields'].map(self.parse_fields).values.tolist())
        mapping_field = {
            900002217983: "channel_inform",
            360003575334: "rating_star",
            360002840393: "platform",
            360002775413: "pic",
            360002859774: "brand",
            360002786334: "issue_type",
            360002859754: "customer_id",
            360002859414: "requested_at",
            360014503253: "responded_at"
        }
        df_fields = df_fields.rename(columns=mapping_field).loc[:, mapping_field.values()]
        df_join = df_cs.join(df_fields)[[
            'id', 'status', 'requester_id', 'assignee_id', 'organization_id', 'group_id',
            'channel_inform', 'rating_star', 'platform', 'pic', 'brand', 'description',
            'issue_type', 'customer_id', 'created_at', 'updated_at', 'requested_at', 'responded_at'
        ]]
        df_join['platform'] = df_join['platform'].str.replace("live_chat__", "")
        df_join['pic'] = df_join['pic'].map(lambda x: x.replace("__", "::").replace('_', ' '))
        df_join['issue_type'] = df_join['issue_type'].map(lambda x: x.replace("__", "::").replace('_', ' '))

        df_join['created_at'] = df_join['created_at'].map(parse_time).map(
            lambda x: x.astimezone(local_tz).replace(tzinfo=None))
        df_join['updated_at'] = df_join['updated_at'].map(parse_time).map(
            lambda x: x.astimezone(local_tz).replace(tzinfo=None))
        df_join['requested_at'] = df_join['requested_at'].str.strip().map(
            lambda x: parse_time(x, '%d-%m-%Y %H:%M', use_utils=False) if not pd.isna(x) else None)
        df_join['requested_at'] = df_join['requested_at'].map(lambda x: self.valid_datetime(x))
        df_join['responded_at'] = df_join['responded_at'].str.strip().map(
            lambda x: parse_time(x, '%d-%m-%Y %H:%M', use_utils=False) if not pd.isna(x) else None)
        df_join['responded_at'] = df_join['responded_at'].map(lambda x: self.valid_datetime(x))
        logger.info("data shape: {}".format(df_join.shape))
        return df_join

    def load_to_database(self, df):
        insert_df_to_postgres(self.postgres_conf, tbl_name="raw_ticket",
                              df=df, primary_keys=['id'], schema="cs")

    @AbstractETL.wrapper_simple_log
    def execute(self, *args):
        logger.info("step 1: extract data from zendesk")
        df_raw = self.extract_raw()
        logger.info("step 2: normalize data")
        df_norm = self.transform(df_raw)
        logger.info("step 3: load to database")
        self.load_to_database(df_norm)


if __name__ == '__main__':
    import json
    conf = {
        "app": {
            "process_type": "etl",
            "process_group": "cs",
            "process_name": "zendesk",
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
    fetch_data = TaskFetchData(config=conf)
    fetch_data.execute()
