import datetime
import functools
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
import tqdm
from dateutil import tz
from pandas import ExcelWriter
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects import postgresql

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()

def merge_dfs(dfs, on_lst, how="outer"):
    return functools.reduce(lambda l, r: pd.merge(l, r, on=on_lst, how=how), dfs)


def count_na(s: pd.Series):
    return len(s[s.isna()])


def count_distinct(s: pd.Series):
    return len(set(s))


def get_postgres_engine(postgres_conf):
    engine = create_engine(
        "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
            user=postgres_conf["user"],
            pw=postgres_conf["password"],
            host=postgres_conf["host"],
            port=postgres_conf["port"],
            db=postgres_conf["database"],
        )
    )
    return engine


def get_postgres_cli(postgres_conf):
    postgres_cli = psycopg2.connect(
        database=postgres_conf["database"],
        user=postgres_conf["user"],
        password=postgres_conf["password"],
        host=postgres_conf["host"],
        port=postgres_conf["port"],
    )
    # with postgres_cli.cursor() as cur:
    #     print('PostgreSQL database version:')
    #     cur.execute('SELECT version()')
    #     db_version = cur.fetchone()
    #     print(db_version)

    return postgres_cli


def load_df_from_postgres(postgres_conf, query):
    engine = get_postgres_engine(postgres_conf)
    df = pd.read_sql(query, engine)
    return df


def insert_df_to_postgres_fast(postgres_conf, tbl_name, df, mode):
    if df.shape[0] == 0:
        return

    engine = get_postgres_engine(postgres_conf)
    if mode == "replace":
        df.to_sql(tbl_name, engine, if_exists="replace", index=False)
    else:
        raise Exception("mode {0} not support".format(mode))


def insert_df_to_postgres(postgres_conf, tbl_name, df, primary_keys: list, schema="public"):
    if df.shape[0] < 1:
        return

    def norm_value(d: dict):
        for k, v in d.items():
            if isinstance(v, (list, tuple)):
                continue
            if pd.isnull(v):
                d[k] = None
        return d

    engine = get_postgres_engine(postgres_conf)
    meta = MetaData(schema=schema)

    conn = engine.connect()
    my_table = Table(tbl_name, meta, autoload=True, autoload_with=conn)
    insert_statement = postgresql.insert(my_table).values(
        list(map(norm_value, df.to_dict(orient="records")))
    )
    if len(primary_keys):
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=primary_keys,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in primary_keys
            },
        )
        conn.execute(upsert_statement)
    else:
        conn.execute(insert_statement)


def save_xls(lst_dfs, xls_path, lst_sheet_name=None):
    with ExcelWriter(xls_path) as writer:
        for n, df in enumerate(lst_dfs):
            try:
                sheet_name = lst_sheet_name[n]
            except Exception as e:
                print(e)
                sheet_name = "sheet_%s" % n

            df.to_excel(writer, sheet_name)
        writer.save()


def get_mongo_cli(mongo_conf):
    uri = "mongodb://%s:%s@%s:%s" % (
        quote_plus(mongo_conf["user"]),
        quote_plus(mongo_conf["password"]),
        mongo_conf["host"],
        mongo_conf["port"],
    )

    client = pymongo.MongoClient(uri)
    return client


def insert_df_into_mongodb(
    mongo_conf, collection_name, df, primary_key="id", upsert=True, batch_size=500
):
    if df.shape[0] < 1:
        return

    mongo_cli = get_mongo_cli(mongo_conf)
    df["updated_at"] = datetime.datetime.now(tz=local_tz)
    if (primary_key is None) or (len(primary_key) == 0):
        mongo_cli[mongo_conf["database"]][collection_name].insert_many(
            df.to_dict(orient="records")
        )
        return
    objects = df.to_dict(orient="records")

    if isinstance(primary_key, str):
        update_objects = list()
        with tqdm.tqdm(total=len(objects)) as pbar:
            for obj in objects:
                update_objects.append(
                    pymongo.UpdateOne(
                        {primary_key: obj[primary_key]}, {"$set": obj}, upsert=upsert
                    )
                )
                if len(update_objects) > batch_size:
                    # mongo_result =
                    mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                        update_objects
                    )
                    # print(mongo_result.bulk_api_result)
                    update_objects = list()
                pbar.update(1)
            if len(update_objects) > 0:
                mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                    update_objects
                )
                # print(mongo_result.bulk_api_result)
                del update_objects
    else:
        assert isinstance(
            primary_key, list
        ), "primary key must be string or list of string, primary key: {}".format(
            primary_key
        )
        update_objects = list()
        with tqdm.tqdm(total=len(objects)) as pbar:
            for obj in objects:
                update_objects.append(
                    pymongo.UpdateOne(
                        {k: obj[k] for k in primary_key}, {"$set": obj}, upsert=upsert
                    )
                )
                if len(update_objects) > batch_size:
                    mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                        update_objects
                    )
                    # print(mongo_result.bulk_api_result)
                    update_objects = list()
                pbar.update(1)
            if len(update_objects) > 0:
                mongo_cli[mongo_conf["database"]][collection_name].bulk_write(
                    update_objects
                )
                # print(mongo_result.bulk_api_result)
                del update_objects


def load_df_from_mongo(
    mongo_conf, collection_name, query, no_id=True, selected_keys=None
):
    mongo_cli = get_mongo_cli(mongo_conf)
    db = mongo_cli[mongo_conf["database"]].with_options(codec_options=codec_options)
    if selected_keys is None:
        cursor = db[collection_name].find(query)
    else:
        cursor = db[collection_name].find(query, selected_keys)
    df = pd.DataFrame(list(cursor))

    if df.shape[0] == 0:
        return df
    # Delete the _id
    if no_id:
        del df["_id"]
    return df


def split_dataframe(df, chunk_size=5000):
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        yield df[i * chunk_size : (i + 1) * chunk_size]
