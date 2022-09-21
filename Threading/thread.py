import threading
import time
import pandas as pd
import concurrent.futures

from pandas.io.parsers import read_csv
t1 = time.perf_counter()

def csv_reader(filename):
    df = pd.read_csv(filename)
    return df


files = ['D:\data_sale\ipython_file\Threading\Rawdata1.csv', 'D:\data_sale\ipython_file\Threading\Rawdata2.csv',
'D:\data_sale\ipython_file\Threading\Rawdata3.csv', 'D:\data_sale\ipython_file\Threading\Rawdata4.csv',
'D:\data_sale\ipython_file\Threading\Rawdata5.csv']


df = pd.DataFrame(columns=['A','B','C'])

# for file in files:
#     result = result.append(csv_reader(file))
#     print(result.shape)
   
if __name__ == '__main__':

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(read_csv, file) for file in files]
        # results = executor.map(csv_reader, files)
        # ProcessPoolExecutor WITH 0.9s

    for f in concurrent.futures.as_completed(results):
        df = df.append(f.result())
        print(df.shape)

    # for result in results:
    #     df = df.append(result)

 
t2 = time.perf_counter()
print(f'Finished in {t2-t1} seconds')

# https://stackoverflow.com/questions/51426533/async-pandas-read-sql-and-asyncio
# https://pythonspeed.com/articles/pandas-sql-chunking/
# https://towardsdatascience.com/optimizing-pandas-read-sql-for-postgres-f31cd7f707ab
