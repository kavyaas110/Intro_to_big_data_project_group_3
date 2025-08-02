"""
CSCI 620
Group 3: Kavyaa Sheth, Pravallika Nakarikanti, Lahari Ijju, and Adwaith Bharath Chandra Togiti
"""

from pymongo import MongoClient
import pandas as pd
from collections import Counter
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth, association_rules

# Connect to MongoDB
username = "kavyaa"
password = "kav110"
host = "localhost"
port = 27017
auth_db = "mydb_proj"

uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_db}"
client = MongoClient(uri, serverSelectionTimeoutMS=5000)
db = client["mydb_proj"] 
titles_collection = db["titles"]
persons_collection = db["persons"]
# Step 1: Load title and director nconsts
title_directors = []
for doc in titles_collection.find(
    {"directors": {"$exists": True, "$ne": []}},
    {"_id": 1, "directors": 1}
):
    title_directors.append((doc["_id"], doc["directors"]))

# Step 2: Build transaction list of director nconsts
tconst_to_directors = {}
all_directors = set()
for tconst, directors in title_directors:
    tconst_to_directors[tconst] = list(set(directors))
    all_directors.update(directors)

# Step 3: Map nconsts to primaryNames in batches
nconst_to_name = {}
BATCH_SIZE = 1000
all_directors = list(all_directors)
for i in range(0, len(all_directors), BATCH_SIZE):
    batch = all_directors[i:i+BATCH_SIZE]
    cursor = persons_collection.find(
        {"_id": {"$in": batch}},
        {"_id": 1, "primaryName": 1}
    )
    for doc in cursor:
        nconst_to_name[doc["_id"]] = doc["primaryName"]

# Step 4: Replace nconsts with names in transactions
transactions = []
for tconst, directors in tconst_to_directors.items():
    names = [nconst_to_name[n] for n in directors if n in nconst_to_name]
    if len(set(names)) >= 2:
        transactions.append(list(set(names)))

print("Step 4 done: Transactions =", len(transactions))

# Step 5: Filter infrequent directors
flat_items = [director for tx in transactions for director in tx]
counts = Counter(flat_items)
min_count = 500 # Need to do this else facing memory issue. The data is too large to be loaded to the ram
frequent_directors = set(director for director, c in counts.items() if c >= min_count)

filtered_transactions = [
    [d for d in tx if d in frequent_directors]
    for tx in transactions
    if sum(d in frequent_directors for d in tx) >= 2
]

print("Step 5 done: Filtered transactions =", len(filtered_transactions))

# Step 6: Transaction encoding
te = TransactionEncoder()
te_ary = te.fit(filtered_transactions).transform(filtered_transactions)
df_tf = pd.DataFrame(te_ary, columns=te.columns_)

# Step 7: Frequent itemsets and rules
frequent_itemsets = fpgrowth(df_tf, min_support=0.001, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.3)

print("Step 7 done: Rules generated =", len(rules))
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))