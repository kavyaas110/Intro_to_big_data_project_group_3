"""
CSCI 620
Group 3: Kavyaa Sheth, Pravallika Nakarikanti, Lahari Ijju, and Adwaith Bharath Chandra Togiti
"""

from pymongo import MongoClient
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth, association_rules
from collections import Counter

# ------------------------
# 1. Connect to MongoDB
# ------------------------
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

# 2. Query: title and producer names
# Fetch all titles with producer IDs
titles_cursor = titles_collection.find(
    {"producers": {"$exists": True, "$ne": []}},
    {"_id": 1, "producers": 1}
)

titles_data = list(titles_cursor)

# Map nconst to primaryName
person_cursor = persons_collection.find(
    {"primaryName": {"$exists": True}},
    {"_id": 1, "primaryName": 1}
)
nconst_to_name = {p["_id"]: p["primaryName"] for p in person_cursor}

# Create transactions: list of producer names per title
transactions = []
for doc in titles_data:
    producer_ids = doc.get("producers", [])
    producers = list(set(nconst_to_name.get(pid) for pid in producer_ids if nconst_to_name.get(pid)))
    if producers:
        transactions.append(producers)

print("Step 2 done: Transactions =", len(transactions))

# 3. Filter infrequent producers
flat_items = [producer for tx in transactions for producer in tx]
counts = Counter(flat_items)
min_count = 500  # adjust based on your data
frequent_producers = set(producer for producer, c in counts.items() if c >= min_count)

filtered_transactions = [
    [producer for producer in tx if producer in frequent_producers]
    for tx in transactions
    if sum(producer in frequent_producers for producer in tx) >= 2
]
print("Step 3 done: Filtered transactions =", len(filtered_transactions))

# 4. Encode and mine
te = TransactionEncoder()
te_ary = te.fit(filtered_transactions).transform(filtered_transactions)
df_tf = pd.DataFrame(te_ary, columns=te.columns_)
print("Step 4 done: Encoded shape =", df_tf.shape)

frequent_itemsets = fpgrowth(df_tf, min_support=0.001, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.3)
print("Step 5 done: Rules generated =", len(rules))

# 5. Output: Co-producer rules
print("\nTop Co-Producer Rules:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))
