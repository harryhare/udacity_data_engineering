import cassandra
from cassandra.cluster import Cluster
cluster=Cluster(["127.0.0.1"])
session=cluster.connect()

session.execute()

query = "CREATE TABLE IF NOT EXISTS music_library"
query = query + "(year int, artist_name text, album_name text, primary key (year,artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query1 = "CREATE TABLE IF NOT EXISTS artist_library"
query1 = query1 + "(year int, artist_name text, album_name text, primary key (artist_name,year))"
try:
    session.execute(query1)
except Exception as e:
    print(e)

query2 = "CREATE TABLE IF NOT EXISTS album_library"
query2 = query2 + "(year int, artist_name text, album_name text, primary key (album_name,artist_name))"
try:
    session.execute(query2)
except Exception as e:
    print(e)
    


###################################################################################
    
query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

query1 = "INSERT INTO artist_library (artist_name,year, album_name)"
query1 = query1 + " VALUES (%s, %s, %s)"

query2 = "INSERT INTO album_library (artist_name, album_name, year)"
query2 = query2 + " VALUES (%s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Who", "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1970, "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("Let it Be", "The Beatles", 1970))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("Rubber Soul", "The Beatles", 1965))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("My Generation", "The Who", 1965))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("The Monkees", "The Monkees", 1966))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Close To You", "The Carpenters", 1970))
except Exception as e:
    print(e)
    
    
    