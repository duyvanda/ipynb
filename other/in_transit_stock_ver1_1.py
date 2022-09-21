import os
from googleapiclient import discovery
from google.oauth2 import service_account
import pandas as pd
import warnings
import psycopg2
from urllib.parse import urlparse
from collector.libs.pandas_utils import insert_df_to_postgres
from email.message import EmailMessage

warnings.filterwarnings('ignore')


def get_postgres_conn(url):
    try:
        result = urlparse(url)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port

        return psycopg2.connect(user=username,
                                password=password,
                                host=hostname,
                                port=port,
                                database=database), {
                                    "host": hostname,
                                    "port": port,
                                    "user": username,
                                    "password": password,
                                    "database": database
                                }
    except (Exception, psycopg2.Error) as error:
        print(error)
        raise


DATABASE_URL = os.getenv('DF', )
connection, postgres_conf = get_postgres_conn(DATABASE_URL)


def check_and_insert(df_raw):
    #Email connection
    EMAIL_ADDRESS = 'asdasdasdasdasdasdasd'
    EMAIL_PASSOWRD = 'sdsdsdsd'
    msg = EmailMessage()
    # FROM EMAIL
    msg['From'] = EMAIL_ADDRESS
    # TO EMAIL
    msg['To'] = ['sdsdsdsdsd']
    # BODY
    msg.set_content('DATA VALIDATION ALERT FROM BI TEAM')

    #connect to speadsheet
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]
    jsonfile = os.path.join(os.getcwd(), 'asdasdasdasasdasd')
    credentials = service_account.Credentials.from_service_account_file(
        jsonfile, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)
    #connect to data in spreadsheets
    spreadsheets_id = 'asdasdasdasd'
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheets_id,
        majorDimension='ROWS',
        range='sdsdsdsdsd').execute()
    #data transform
    columns = response['values'][1]
    data = [item for item in response['values'][2:]]
    df_raw = pd.DataFrame(data, columns=columns)
    df_raw['In-transit_Q.ty'] = pd.to_numeric(
        df_raw['In-transit_Q.ty'], errors='coerce').fillna(0).astype('int64')
    df_raw['WH Transfer'] = pd.to_numeric(
        df_raw['WH Transfer'], errors='coerce').fillna(0).astype('int64')
    df_raw.columns = [
        'brand', 'po', 'sku', 'intransit_suppliers_to_op',
        'intransit_3pl_warehouse_to_platform_warehouse'
    ]

    #data validation
    duplicates = df_raw.duplicated(subset=['po', 'sku']).sum()
    wrong_intransit_qty = (df_raw['intransit_suppliers_to_op'] < 0).sum()
    wrong_whtransfer_qty = (
        df_raw['intransit_3pl_warehouse_to_platform_warehouse'] < 0).sum()
    if duplicates > 0:
        msg['Subject'] = '[ALERT] IN TRANSIT df has duplicate values'
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSOWRD)
            smtp.send_message(msg)
        assert duplicates == 0, 'df has duplicate values'
    elif wrong_intransit_qty > 0:
        msg['Subject'] = '[ALERT] IN TRANSIT df has wrong qty input'
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSOWRD)
            smtp.send_message(msg)
        assert wrong_intransit_qty == 0, 'df has wrong values at wrong_intransit_qty'
    elif wrong_whtransfer_qty > 0:
        msg['Subject'] = '[ALERT] IN TRANSIT df has wrong qty input'
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSOWRD)
            smtp.send_message(msg)
        assert wrong_whtransfer_qty == 0, 'df has wrong values at wrong_whtransfer_qty'
    else:
        msg['Subject'] = '[ALERT] IN TRANSIT QTY FROM SC, ALL VALIDATION PASSED'
        print("\033[92m No duplicate found")
        print("\033[92m No wrong values at wrong_intransit_qty")
        print("\033[92m No wrong values at wrong_whtransfer_qty")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSOWRD)
            smtp.send_message(msg)
        insert_df_to_postgres(postgres_conf,
                              tbl_name="sdsdsdsdsdssdsd",
                              df=df_raw,
                              primary_keys=['po', 'sku', 'inserted_at'],
                              schema="public")


check_and_insert()