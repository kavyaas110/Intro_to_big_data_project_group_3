"""
CSCI 620
Group 3: Kavyaa Sheth, Pravallika Nakarikanti, Lahari Ijju, and Adwaith Bharath Chandra Togiti
"""
from pymongo import MongoClient
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules

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

# ------------------------
# 2. Fetch tconst and genres
# ------------------------
cursor = titles_collection.find(
    {
        "genres": {"$exists": True, "$ne": []}
    },
    {
        "tconst": 1,
        "genres": 1
    }
)

data = list(cursor)
print("Step 2 done: ", len(data))

# ------------------------
# 3. Group by tconst → genre list per title
# ------------------------
transactions = [doc["genres"] for doc in data if isinstance(doc["genres"], list)]
print("Step 3 done")

# ------------------------
# 4. One-hot encode transactions
# ------------------------
te = TransactionEncoder()
te_ary = te.fit(transactions).transform(transactions)
df_tf = pd.DataFrame(te_ary, columns=te.columns_)
print("Step 4 done")

# ------------------------
# 5. Mine frequent itemsets & rules
# ------------------------
frequent_itemsets = apriori(df_tf, min_support=0.01, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.5)
print("Step 5 done")

# ------------------------
# 6. Output results
# ------------------------
print("Top Frequent Genre Combinations:")
print(frequent_itemsets.sort_values(by='support', ascending=False).head(10))

print("\nTop Association Rules:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))
