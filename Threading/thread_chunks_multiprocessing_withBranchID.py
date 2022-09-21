import multiprocessing
import time
import pyodbc
import pandas as pd
import concurrent.futures

from pandas.io.parsers import read_csv

t1 = time.perf_counter()

def worker(y):
    query = "SELECT * from OM_SalesOrdDet WHERE BranchID = '{0}'".format(y)
    cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', 
                      host=server, database=db1, trusted_connection=tcon,
                      user=uname, password=pword)
    return pd.read_sql(query, cnxn)

server = '115.165.164.234'
driver = 'SQL Server'
db1 = 'PhaNam_eSales_PRO'
tcon = 'no'
uname = 'duyvq'
pword = '123VanQuangDuy'
cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', 
                      host=server, database=db1, trusted_connection=tcon,
                      user=uname, password=pword)

gens = ['MR0001', 'MR0003', 'MR0010', 'MR0011', 'MR0012', 'MR0013', 'MR0014', 'MR0015', 'MR0016']

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
