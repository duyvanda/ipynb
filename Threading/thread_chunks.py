import threading
import time
import pyodbc
import pandas as pd
import concurrent.futures

from pandas.io.parsers import read_csv

t1 = time.perf_counter()

def to_df(df):
    # df.to_csv(f'note{index}.csv')
    return df

server = '115.165.164.234'
driver = 'SQL Server'
db1 = 'PhaNam_eSales_PRO'
tcon = 'no'
uname = 'duyvq'
pword = '123VanQuangDuy'
cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', 
                      host=server, database=db1, trusted_connection=tcon,
                      user=uname, password=pword)

query = "SELECT * from OM_SalesOrdDet"
gens = pd.read_sql(query, cnxn, chunksize=30000)

df = pd.DataFrame()

if __name__ == '__main__':
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # for gen in gens:
        #     # result = executor.submit(to_csv, elements, index)
        #     result = executor.submit(to_df, gen)
        #     results.append(result)
        results = executor.map(to_df, gens)
        # # ProcessPoolExecutor WITH 0.9s

    # for f in concurrent.futures.as_completed(results):
    #     df = df.append(f.result())
    #     print(df.shape)

    for result in results:
        df = df.append(result)
        print(df.shape)
        
cnxn.close
t2 = time.perf_counter()
print(f'Finished in {t2-t1} seconds')

# https://stackoverflow.com/questions/51426533/async-pandas-read-sql-and-asyncio
# https://pythonspeed.com/articles/pandas-sql-chunking/
# https://towardsdatascience.com/optimizing-pandas-read-sql-for-postgres-f31cd7f707ab
# https://stackoverflow.com/questions/51152183/fastest-way-to-read-huge-mysql-table-in-python
