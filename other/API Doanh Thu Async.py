import sys
import pandas as pd
import pyodbc
import asyncio
print(sys.executable)

server = '115.165.164.234'
driver = 'SQL Server'
db1 = 'PhaNam_eSales_PRO'
tcon = 'no'
uname = 'duyvq'
pword = '123VanQuangDuy'

dates_list = ['20210813']

cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                      host=server, database=db1, trusted_connection=tcon,
                      user=uname, password=pword)

columns = ['Notes', 'CustomerCode', 'CustomerName', 'CustomerInvoiceCode',
       'CustomerInvoice', 'AccountDebit', 'AccountCredit', 'CashPayable',
       'Unit', 'CashNumber', 'DateReceive', 'IDCodeOrder', 'DateOrder',
       'Symbols', 'InvoiceNumber', 'DateInvoice', 'SalesChannel', 'SaleManID',
       'SaleMan', 'DelivererID', 'Deliverer', 'MemberAddID', 'MemberAdd',
       'ReceiverID', 'Receiver', 'OfficeCode', 'Office']

# df = pd.DataFrame(columns=columns)


async def get_data(date):
    query = f"EXEC [Api_GetARDoc] @DateGetData={date}"
    df = pd.read_sql(query, cnxn)
    return df

dates_list = ['20210811','20210812']
async def main(date):
    tasks = []
    for date in dates_list:
        task = asyncio.create_task(get_data(date))
        tasks.append(task)
    await asyncio.gather(*tasks)
    print(df.shape)


asyncio.run(main(f"'20210813'"))