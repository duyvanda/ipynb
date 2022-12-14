# %%
from utils.df_handle import *
import requests
from requests.structures import CaseInsensitiveDict

# %% [markdown]
# import pendulum
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# 
# local_tz = pendulum.timezone("Asia/Bangkok")
# 
# name='CareSoftCustomer'
# prefix='Crawl_'
# csv_path = '/usr/local/airflow/plugins'+'/'
# 
# dag_params = {
#     'owner': 'airflow',
#     "depends_on_past": False,
#     'start_date': datetime(2022, 5, 14, tzinfo=local_tz),
#     # 'email_on_failure': True,
#     # 'email_on_retry': False,
#     # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
#     'do_xcom_push': False,
#     'execution_timeout':timedelta(seconds=300)
#     # 'retries': 3,
#     # 'retry_delay': timedelta(minutes=10),
# }
# 
# dag = DAG(prefix+name,
#           catchup=False,
#           default_args=dag_params,
#           schedule_interval= '*/30 8-20,23-23 * * *',
#           tags=[prefix+name, 'Sync', '30mins']
# )

# %%
data1 = {"from":1643652037221,"to":1656608154405}
data2 = {"from": 1656612054988, "to": 1667235316382}
list_dict = [data1, data2]
list_pickle_pathname = ["part1.pickle", "part2.pickle"]

# %%
def get_data(pickle_pathname: str, data: dict):
    url = "https://web11.caresoft.vn/admin/user/export"
    headers = CaseInsensitiveDict()
    headers['authority'] = 'web11.caresoft.vn'
    headers['accept'] = 'application/json, text/plain, */*'
    headers['content-type'] = 'application/json;charset=UTF-8'
    headers['cookie'] = '_fbp=fb.1.1663812395640.118225221; _ga=GA1.1.296298468.1663812395; remember_82e5d2c56bdd0811318f0cf078b78bfc=eyJpdiI6ImEzOVhXYUpKZ0xaTzJWbWtPXC9rVGNnPT0iLCJ2YWx1ZSI6Ikt1ZnlYVVJldzJwMllQTUhpVkVcL0dnWHV6dG1UKzl1VWFCN2gxTm1ka0UrXC9LOWpsM3VzNDZyeEtkXC9oTW83MHlaQ0dKU3NkQU5tUmdNZ2VOYVgrS0hkSGdOcEFKdWRGa1Z0OTdaR0VZa293PSIsIm1hYyI6ImU2OWVlMzU3MWM5OTExZjc3NGRmYWZkZmMyZjU3NzQ5ODVjODMwMGE2ODMwOGRmMDlmYWY1MWRkZWYxNzhlOTkifQ%3D%3D; _ga_TXHEXQXK1P=GS1.1.1666680497.4.1.1666682046.0.0.0; laravel_session=eyJpdiI6IjIrUWZoMkUxS05ZeEdsSVYzRUxGTlE9PSIsInZhbHVlIjoiSm9zaGlubG9ZaTk2SVZqdkpMZlVsbUdxZ2gzd0ZQbW5XK3Mxbkh5ZmR1NDg4S1JlMGNhM2t5RmNNUUJXV1lSYUZQQ245WGF2ck1hNzZGaWE5M0laUUE9PSIsIm1hYyI6ImI3Yzc5YTk3ZGU2YmVjNWM2NzA4MmU5ZTBmZjFhN2E0ZjZlM2Y5NTBiNzcyMTJmODgzYzFkNDgzZTI0NDFlZjcifQ%3D%3D'
    headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    resp = requests.post(url, headers=headers, json=data)
    df = pd.read_excel(resp.json()['status']['extraData']['urlToFile'])
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df.to_pickle(pickle_pathname)

# %%
zipped = zip(list_dict, list_pickle_pathname)
for tuple in zipped:
    print(tuple[0], tuple[1])
    get_data(tuple[1], tuple[0])
    # print(tuple[0], tuple[1])

# %% [markdown]
# dummy_start = DummyOperator(task_id="dummy_start", dag=dag)
# 
# dummy_end = DummyOperator(task_id="dummy_end", dag=dag)
# 
# insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)
# 
# update = PythonOperator(task_id="update", python_callable=update, dag=dag)
# 
# dummy_start >> update >> insert >> dummy_end


