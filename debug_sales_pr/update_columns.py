# Ran into a similar problem and the current accepted solution was too slow for me. My table had 500k+ rows and i needed to update 100k+ rows. After lengthy research and trial and error i arrived at an efficient and correct solution.

# The idea is to use psycopg as your writer and to use a temp table. df is your pandas dataframe that contains values you want to set.

import psycopg2

conn = psycopg2.connect("dbname='db' user='user' host='localhost' password='test'")
cur = conn.cursor()

rows = zip(df.id, df.z)
cur.execute("""CREATE TEMP TABLE codelist(id INTEGER, z INTEGER) ON COMMIT DROP""")
cur.executemany("""INSERT INTO codelist (id, z) VALUES(%s, %s)""", rows)

cur.execute("""
    UPDATE table_name
    SET z = codelist.z
    FROM codelist
    WHERE codelist.id = vehicle.id;
    """)

cur.rowcount
conn.commit()
cur.close()
conn.close()