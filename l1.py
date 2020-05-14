import psycopg2
conn = psycopg2.connect("host=127.0.0.1 port=5434 dbname=studentdb user=student password=student")
cur = conn.cursor()
conn.set_session(autocommit=True)

try:
    cur.execute("select * from test1")
except:
    conn.commit()
    pass
cur.execute("CREATE TABLE test1 (col1 int, col2 int, col3 int);")
conn.commit()

