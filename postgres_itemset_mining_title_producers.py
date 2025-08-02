"""
CSCI 620
Group 3: Kavyaa Sheth, Pravallika Nakarikanti, Lahari Ijju, and Adwaith Bharath Chandra Togiti
"""

import psycopg2
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
from collections import Counter
from mlxtend.frequent_patterns import fpgrowth

# ------------------------
# 1. Connect to PostgreSQL
# ------------------------
conn = psycopg2.connect(
    dbname="myimdbproj",
    user="kavyaa",
    password="1234",
    host="localhost",
    port="5432"
)

# 2. Query: Title comes first, then producers
query = """
SELECT t.tconst, p.primaryname AS producername
FROM Title t
JOIN Produces pr ON pr.tconst = t.tconst
JOIN Person p ON p.nconst = pr.nconst
WHERE p.primaryname IS NOT NULL
"""
df = pd.read_sql(query, conn)
print("Step 2 done: Rows fetched =", len(df))

# 3. Group producers per title
grouped = df.groupby('tconst')['producername'].apply(lambda x: list(set(x)))
transactions = grouped.tolist()
print("Step 3 done: Transactions =", len(transactions))

# 4. Filter infrequent producers
flat_items = [producer for tx in transactions for producer in tx]
counts = Counter(flat_items)
min_count = 500  # adjust based on your data
frequent_producers = set(producer for producer, c in counts.items() if c >= min_count)

filtered_transactions = [
    [producer for producer in tx if producer in frequent_producers]
    for tx in transactions
    if sum(producer in frequent_producers for producer in tx) >= 2  # skip solo-producer
]
print("Step 4 done: Filtered transactions =", len(filtered_transactions))

# 5. Encode transactions (dense)
te = TransactionEncoder()
te_ary = te.fit(filtered_transactions).transform(filtered_transactions)
df_tf = pd.DataFrame(te_ary, columns=te.columns_)
print("Step 5 done: Encoded shape =", df_tf.shape)

# 6. Frequent itemsets & rules
frequent_itemsets = fpgrowth(df_tf, min_support=0.001, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.3)
print("Step 6 done: Rules generated =", len(rules))

# 7. Output: Co-producer rules
print("\nTop Co-Producer Rules:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))
