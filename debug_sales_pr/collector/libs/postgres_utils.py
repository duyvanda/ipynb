import pandas as pd
import psycopg2.extras
from collector.libs.pandas_utils import get_postgres_cli


def get_tables_postgres(postgres_conf):
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            query = """
            SELECT table_schema || '.' || table_name as "table_name"
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')"""
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())


def change_owner_postgres(postgres_conf, to_owner):
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor() as cursor:
            query = """ALTER TABLE mapping_user_id_sendovn OWNER TO {0}""".format(
                to_owner
            )
            cursor.execute(query)


def create_user_postgres(postgres_conf, user, password):
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor() as cursor:
            query = """CREATE USER {0} WITH PASSWORD '{1}'""".format(user, password)
            cursor.execute(query)
            ps_cli.commit()


def get_users_postgres(postgres_conf):
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            query = """SELECT u.usename AS "User Name" FROM pg_catalog.pg_user u"""
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())


def grant_all_postgres(postgres_conf, user, db):
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor() as cursor:
            query = """GRANT CONNECT ON DATABASE {0} TO {1}""".format(db, user)
            cursor.execute(query)
            query = "GRANT USAGE ON SCHEMA public TO {0}".format(user)
            cursor.execute(query)
            query = "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {0};".format(
                user
            )
            cursor.execute(query)
            query = "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {0}".format(
                user
            )
            cursor.execute(query)
            query = """GRANT ALL PRIVILEGES ON DATABASE {0} to {1}""".format(db, user)
            cursor.execute(query)
            ps_cli.commit()


def get_grant_postgres(postgres_conf, grantee):
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            query = """
            SELECT grantee, privilege_type 
            FROM information_schema.role_table_grants WHERE grantee='{}' """.format(
                grantee
            )
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())


# def get_df_postgres(postgres_conf, query):
#     with get_postgres_cli(postgres_conf) as ps_cli:
#         with ps_cli.cursor() as cursor:
#             cursor.execute(query)
#             return pd.DataFrame(cursor.fetchall())


def get_primary_key_postgres(postgres_conf, table, schema="public"):
    query = """
    SELECT               
      pg_attribute.attname, 
      format_type(pg_attribute.atttypid, pg_attribute.atttypmod) 
    FROM pg_index, pg_class, pg_attribute, pg_namespace 
    WHERE 
      pg_class.oid = '{0}'::regclass AND 
      indrelid = pg_class.oid AND 
      nspname = '{1}' AND 
      pg_class.relnamespace = pg_namespace.oid AND 
      pg_attribute.attrelid = pg_class.oid AND 
      pg_attribute.attnum = any(pg_index.indkey)
     AND indisprimary
    """.format(
        table, schema
    )
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())


def get_table_datatype(postgres_conf, tbl_name):
    query = """select column_name, data_type from information_schema.columns 
    where table_name = '{0}';""".format(
        tbl_name
    )
    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())


def get_list_primary(postgres_conf, tbl_name, schema='public'):
    query = """
    SELECT a.attname as "columns"
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                         AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = '{0}.{1}'::regclass
    AND    i.indisprimary;
    """.format(schema, tbl_name)

    with get_postgres_cli(postgres_conf) as ps_cli:
        with ps_cli.cursor() as cursor:
            cursor.execute(query)
            rs = list()
            for icol in cursor.fetchall():
                rs.append(icol[0])
            return rs
