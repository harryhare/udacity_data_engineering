import cassandra

from cassandra.cluster import Cluster

try:
    cluster = Cluster(['127.0.0.1'])  # If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

try:
    session.set_keyspace('udacity')
except Exception as e:
    print("set keyspace error")
    print(e)

try:
    session.execute("""select * from music_libary""")
except Exception as e:
    print("select error")
    print(e)
