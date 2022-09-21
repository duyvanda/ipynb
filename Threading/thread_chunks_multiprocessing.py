import multiprocessing
import time
import pyodbc
import pandas as pd
import concurrent.futures

from pandas.io.parsers import read_csv

t1 = time.perf_counter()

def worker(y):
    # df.to_csv(f'note{index}.csv')
    return y

server = '115.165.164.234'
driver = 'SQL Server'
db1 = 'PhaNam_eSales_PRO'
tcon = 'no'
uname = 'duyvq'
pword = '123VanQuangDuy'
cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', 
                      host=server, database=db1, trusted_connection=tcon,
                      user=uname, password=pword)

query = "SELECT * from OM_PDASalesOrd"
gens = pd.read_sql(query, cnxn, chunksize=10000)

df = pd.DataFrame()
if __name__ == '__main__':
    p = multiprocessing.Pool(processes=10)
    data = p.map(worker, [gen for gen in gens])
    p.close()
    results = pd.concat(data) 
    cnxn.close
t2 = time.perf_counter()
print(f'Finished in {t2-t1} seconds')

# https://stackoverflow.com/questions/51426533/async-pandas-read-sql-and-asyncio
# https://pythonspeed.com/articles/pandas-sql-chunking/
# https://towardsdatascience.com/optimizing-pandas-read-sql-for-postgres-f31cd7f707ab
