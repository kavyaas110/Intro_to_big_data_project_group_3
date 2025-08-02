"""
CSCI 620
Group 3: Kavyaa Sheth, Pravallika Nakarikanti, Lahari Ijju, and Adwaith Bharath Chandra Togiti
"""

import psycopg2
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
from collections import Counter

# 1. Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="myimdbproj",
    user="kavyaa",
    password="1234",
    host="localhost",
    port="5432"
)
# 2. Query: Title comes first
query = """
SELECT t.tconst, g.genreName
FROM Title t
JOIN Has_Genre hg ON hg.tconst = t.tconst
JOIN Genre g ON hg.genreID = g.genreID
WHERE g.genreName IS NOT NULL
"""
df = pd.read_sql(query, conn)
print("Step 2 done: ",len(df))

# 3. Group by tconst in Python to form genre lists per title
transactions = df.groupby('tconst')['genrename'].apply(list).tolist()
print("Step 3 done")
# 4. Encode transactions into one-hot format
te = TransactionEncoder()
te_ary = te.fit(transactions).transform(transactions)
df_tf = pd.DataFrame(te_ary, columns=te.columns_)
print("Step 4 done")
# 5. Frequent Itemsets
frequent_itemsets = apriori(df_tf, min_support=0.01, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.5)
print("Step 5 done")
# 6. Output
print("Top Frequent Genre Combinations:")
print(frequent_itemsets.sort_values(by='support', ascending=False).head(10))

print("\nTop Association Rules:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))