"""
CSCI 620 
Project Phase 2
Group 3: Kavyaa Sheth, Lahari Ijju, Pravallika Nakarikanti, and Adwaith Bharath Chandra Togiti
"""

import psycopg2
from pymongo import MongoClient
from bson import Int64
import json

def connect_to_postgres():
    # Reading data from relational database
    # Connecting to our Postgres Database to read the data to be loaded
    conn_string = "host='localhost' dbname='myimdbproj' user='kavyaa' password='1234'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print("Connected to postgres!\n")
    return cursor

def connect_to_mongodb():
    # Connecting to MongoDB
    username = "kavyaa"
    password = "kav110"
    host = "localhost"
    port = 27017
    auth_db = "mydb_proj"

    uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_db}"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    print("Connected to MongoDB!")
    # Access a database
    db = client["mydb_proj"]

    return db

def load_person_data(cursor,db):
    # Loading the data from person table from postgresql
    query = "SELECT * FROM person"
    cursor.execute(query)
    rows = cursor.fetchall()

    # transforming it to json
    person_data = []
    print("Number of person data read: ",len(rows))
    for row in rows:
        doc = {
            "_id": Int64(row[0]),
            "primaryName": str(row[1]),
            "birthYear": row[2],
            "deathYear": row[3]
        }
        person_data.append(doc)

    size = len(person_data)
    print("Number of person data prepared for mongo: ",size)

    # Access persons collections and writing transformed data as mongoDB documents
    # We made batches because there are millions of rows
    collection = db["persons"]
    start = 0
    for i in range(10000,size,10000):
        batch = person_data[start:i]
        collection.insert_many(batch)
        print("Completed Loading ",i," rows of person data...")
        start = i
    batch = person_data[start:]
    collection.insert_many(batch)
    print("Loading Person Data Complete")
    count = collection.count_documents({})
    print("Total documents:", count)

def load_title_data(cursor,db):
    # Loading the data from title and many other tables from which we need related data from postgresql
    # We use ARRAY_AGG to gives us array of nconst for same tconst. We use ARRAY_REMOVE to remove null so instead of having [null], we would have []
    query = """SELECT t.tconst,
                t.primaryTitle, 
                t.originaltitle,
                t.titletype,
                t.isadult,
                t.startyear,
                t.endyear,
                t.runtimeminutes,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT h.genrename), NULL) AS genres,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.nconst), NULL) AS actors,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT d.nconst), NULL) AS directors,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT p.nconst), NULL) AS producers,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT w.nconst), NULL) AS writers
            FROM title t
            LEFT JOIN acts_in a ON t.tconst = a.tconst
            LEFT JOIN directs d ON t.tconst = d.tconst
            LEFT JOIN produces p ON t.tconst = p.tconst
            LEFT JOIN writes w ON t.tconst = w.tconst
            LEFT JOIN has_genre h ON t.tconst = h.tconst
            GROUP BY t.tconst;"""
    cursor.execute(query)
    rows = cursor.fetchall()

    # transforming it to json
    title_data = []
    print("Number of person data read: ",len(rows))
    for row in rows:
        doc = {
            "_id": Int64(row[0]),
            "primaryTitle": row[1], 
            "originalTitle": row[2],
            "titleType": row[3],
            "isAdult": row[4],
            "startYear": row[5],
            "endYear": row[6],
            "runtimeMinutes": row[7],
            "genres": row[8],
            "actors": [Int64(val) for val in row[9]],
            "directors": [Int64(val) for val in row[10]],
            "producers": [Int64(val) for val in row[11]],
            "writers": [Int64(val) for val in row[12]]
        }
        title_data.append(doc)

    size = len(title_data)
    print("Number of title data prepared: ",size)

    with open("output.json", "w") as f:
        json.dump(title_data, f, indent=2)

    # Access titles collections and writing transformed data as mongoDB documents
    # We made batches because there are millions of rows
    collection = db["titles"]

    start = 0
    for i in range(10000,size,10000):
        batch = title_data[start:i]
        collection.insert_many(batch)
        print("Completed Loading ",i," rows of titles data...")
        start = i
    batch = title_data[start:]
    collection.insert_many(batch)
    print("Titles Loading Data Complete")
    count = collection.count_documents({})
    print("Total documents:", count)

def load_ratings_data(cursor,db):
    # Loading the data from rating table from postgresql
    query = "SELECT * FROM rating"
    cursor.execute(query)
    rows = cursor.fetchall()

    # transforming it to json
    rating_data = []
    print("Number of rating data read: ",len(rows))
    for row in rows:
        doc = {
            "_id": Int64(row[0]),
            "averageRating": float(row[1]),
            "numVotes": row[2]
        }
        rating_data.append(doc)

    size = len(rating_data)
    print("Number of rating data prepared for mongo: ",size)

    # Access ratings collections and writing transformed data as mongoDB documents
    # We made batches because there are millions of rows
    collection = db["ratings"]
    start = 0
    for i in range(10000,size,10000):
        batch = rating_data[start:i]
        collection.insert_many(batch)
        print("Completed Loading ",i," rows of rating data...")
        start = i
    batch = rating_data[start:]
    collection.insert_many(batch)
    print("Loading Rating Data Complete")
    count = collection.count_documents({})
    print("Total documents:", count)

postgres_cursor = connect_to_postgres()
mongodb_db = connect_to_mongodb()

load_person_data(postgres_cursor,mongodb_db)
load_title_data(postgres_cursor,mongodb_db)
load_ratings_data(postgres_cursor,mongodb_db)