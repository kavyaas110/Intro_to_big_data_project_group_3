"""
CSCI 620
Group 3: Kavyaa Sheth, Lahari Ijju, Pravallika Nakarikanti, and Adwaith Bharath Chandra Togiti
"""

import pandas as pd

# Load the required files for preprocessing into pandas dataframe
df_person = pd.read_csv("name.basics.tsv", sep='\t', encoding='utf-8')
df_title = pd.read_csv("title.basics.tsv", sep='\t', encoding='utf-8')
df_ratings = pd.read_csv("title.ratings.tsv", sep='\t', encoding='utf-8')
df_principals = pd.read_csv("title.principals.tsv", sep='\t', encoding='utf-8')
df_crew = pd.read_csv("title.crew.tsv", sep='\t', encoding='utf-8')

# PRE-PROCESSING FOR PERSON TABLE
df_person = df_person.drop(columns=['primaryProfession','knownForTitles'])  # dropping unwanted columns
df_person['nconst'] = df_person['nconst'].str.replace(r'^nm', '', regex=True)   # Removing the prefix nm as our primary key is of Integer type
df_person['nconst'] = df_person['nconst'].astype(int)   #Type casting it to Integer
valid_nconst_set = set(df_person['nconst']) # We need variable for preprocessing of other tables
print("Person Data: ",len(df_person))   # Printing number of rows in this file. We will verify that same number of lines are also copied using \copy command
df_person.to_csv("cleaned_name_basics.tsv", sep='\t', index=False, encoding='utf-8')    # Writing the processed data into a file. \copy command will use this file to copy the data to the database.



# PRE-PROCESSING FOR TITLE TABLE
df_title['tconst'] = df_title['tconst'].str.replace(r'^tt', '', regex=True) # Removing the prefix tt as our primary key is integer
df_title['tconst'] = df_title['tconst'].astype(int)   #Type casting it to Integer
valid_tconst_set = set(df_title['tconst']) # We need this variable for pre processing of other tables

# For about 66198  rows for which the split is not happening properly. It stores the whole string with "\t" in original title field so
# dtaframe has one less value and then it creates issues while copying to postgres because postgres splits it on that. 
# So to deal with the data issue this code makes sure that split happens on "\t" and then moves the values to right side
# Eg if my file contains a row "tt00001 \t shirt \t Le clown et ses chiens\tLe clown et ses chiens \t 0 \t 1892 \t \N \t 5 \t Animation,Short". 
# Ideally this should be loaded as:
# tconst                         tt0000002
# titleType                          short
# primaryTitle      Le clown et ses chiens
# originalTitle     Le clown et ses chiens
# isAdult                                0
# startYear                           1892
# endYear                               \N
# runtimeMinutes                         5
# genres                   Animation,Short
# But insrtead due to "Le clown et ses chiens\tLe clown et ses chiens" instead of "Le clown et ses chiens \t Le clown et ses chiens" it ends up loading:
# tconst                         tt0000002
# titleType                          short
# primaryTitle      Le clown et ses chiens\tLe clown et ses chiens
# originalTitle                          0
# isAdult                             1892
# startYear                             \N
# endYear                                5
# runtimeMinutes           Animation,Short
# genres                               NaN
# Hence to solve this issue using the code below. 

insert_col = 3  # Index of the column where issue is happening for ~66k rows. We want to shift values to right from here
for i in df_title.index[df_title['genres'].isna()]: # This condition makes sure that we are only altering affected rows.
    for col in range(8, insert_col, -1): # Shifting all values to right
        df_title.iloc[i, col] = df_title.iloc[i, col - 1]
    new_val1,new_val2 = df_title.iloc[i,2].split("\t")  # Storing the right values in respective columns
    df_title.iloc[i, insert_col] = new_val2
    df_title.iloc[i,2] = new_val1

# Just to validate the isAdult field has only 0, 1 or \N as its value
unique_values = df_title["isAdult"].unique()
print("Unique values:", unique_values)

df_genre = df_title.copy()    # copying the dataframe before we drop any columns as we will be using this for other relations
df_title = df_title.drop(columns=['genres']) # Dropping unwanted columns
print("Title Data: ",len(df_title))   # Printing number of rows in this file. We will verify that same number of lines are also copied using \copy command
df_title.to_csv("cleaned_title_basics.tsv", sep='\t', index=False, encoding='utf-8') # Writing the processed data into a file. \copy command will use this file to copy the data to the database.

# PRE-PROCESSING FOR GENRE and HAS_GENRE TABLE 
# For Genre we only need a list of unique genres and then we use insert INTO command as there were just 28 values and it felt like a overkill to make a file for that.
# The following piece of codes gives those unique genres. 
# We also need title to genre mapping and this code also deals with that. We use that file to populate HAS_GENRE table
df_genre['genres'] = df_genre['genres'].dropna().apply(lambda x: x.split(','))
unique_genres = set(genre for sublist in df_genre['genres'] for genre in sublist)
print(unique_genres)

df_genre_title = df_genre.drop(columns=['titleType','primaryTitle','originalTitle','isAdult','startYear','endYear','runtimeMinutes'])
# We want title id - genre mapping, so we need to split (id1 ["genre1","genre2","genre3"]) into (id1 "genre1",id1 "genre2", id1 "genre3")
df_genre_title = df_genre_title.explode('genres')  
df_genre_title = df_genre_title[df_genre_title['genres'] != '\\N'] # Remove rows with null genres
print("Has_Genre Data: ",len(df_genre_title))   # Printing number of rows in this file. We will verify that same number of lines are also copied using \copy command
df_genre_title.to_csv("cleaned_genre_title_mapping.tsv", sep='\t', index=False, encoding='utf-8') # Writing the processed data into a file. \copy command will use this file to copy the data to the database.



# PRE-PROCESSING FOR RATING TABLE
df_ratings['tconst'] = df_ratings['tconst'].str.replace(r'^tt', '', regex=True) # Removing the prefix tt as our primary key is integer
df_title['tconst'] = df_title['tconst'].astype(int)   #Type casting it to Integer
print("Rating Data: ",len(df_ratings))   # Printing number of rows in this file. We will verify that same number of lines are also copied using \copy command
df_ratings.to_csv("cleaned_title_ratings.tsv", sep='\t', index=False, encoding='utf-8')  # Writing the processed data into a file. \copy command will use this file to copy the data to the database.



# PRE-PROCESSING FOR ACTS_IN, DIRECTS, WRITES AND PRODUCES TABLE
df_crew.replace('\\N', pd.NA, inplace=True) # We did this so we can use dropna function of pandas

# Director and Writer data is being fetched from two files - title.crew.tsv and title.principals.tsv. Actor and Producer data comes from title.principals.tsv
df_directors =  df_crew[['tconst', 'directors']].dropna(subset=['directors'])
df_writer = df_crew[['tconst', 'writers']].dropna(subset=['writers'])

# Renaming columns so we can merge it with other dataframe later
df_directors = df_directors.rename(columns={'directors':'nconst'})
df_writer = df_writer.rename(columns={'writers':'nconst'})

df_directors['nconst'] = df_directors['nconst'].apply(lambda x: x.split(','))
df_directors = df_directors.explode('nconst') 

df_writer['nconst'] = df_writer['nconst'].apply(lambda x: x.split(','))
df_writer = df_writer.explode('nconst') 

df_principals = df_principals.drop(columns=['ordering','job','characters']) # dropping unwanted columns

# We filter based on dataframe we are making and make sure ids are integer. 
new_directors = df_principals[df_principals['category'] == 'director'][['tconst', 'nconst']]
df_directors = pd.concat([df_directors, new_directors], ignore_index=True)
df_directors = df_directors.drop_duplicates()
df_directors['tconst'] = df_principals['tconst'].str.replace(r'^tt', '', regex=True).astype(int) 
df_directors['nconst'] = df_principals['nconst'].str.replace(r'^nm', '', regex=True).astype(int) 
df_directors = df_directors.drop_duplicates()

new_writers = df_principals[df_principals['category'] == 'writer'][['tconst', 'nconst']]
df_writer = pd.concat([df_writer, new_writers], ignore_index=True)
df_writer = df_writer.drop_duplicates()
df_writer['tconst'] = df_writer['tconst'].str.replace(r'^tt', '', regex=True).astype(int) 
df_writer['nconst'] = df_writer['nconst'].str.replace(r'^nm', '', regex=True).astype(int) 

df_producers = df_principals[df_principals['category'] == 'producer'][['tconst', 'nconst']]
df_producers = df_producers.drop_duplicates()
df_producers['tconst'] = df_producers['tconst'].str.replace(r'^tt', '', regex=True).astype(int) 
df_producers['nconst'] = df_producers['nconst'].str.replace(r'^nm', '', regex=True).astype(int) 

df_actors = df_principals[df_principals['category'].isin(['actor', 'actress'])][['tconst', 'nconst']]
df_actors = df_actors.drop_duplicates()
df_actors['tconst'] = df_actors['tconst'].str.replace(r'^tt', '', regex=True).astype(int) 
df_actors['nconst'] = df_actors['nconst'].str.replace(r'^nm', '', regex=True).astype(int) 

print("Directors Data Before validating FK: ",len(df_directors))
print("Writers Data Before validating FK: ",len(df_writer))
print("Producers Data Before validating FK: ",len(df_producers))
print("Actors Data Before validating FK: ",len(df_actors))

# We need to filter out rows who have ids which don't exist in original table. Otherwise it will lead to Foreign Key violation
df_directors = df_directors[df_directors['nconst'].isin(valid_nconst_set) & df_directors['tconst'].isin(valid_tconst_set)]
df_writer = df_writer[df_writer['nconst'].isin(valid_nconst_set) & df_writer['tconst'].isin(valid_tconst_set)]
df_producers = df_producers[df_producers['nconst'].isin(valid_nconst_set) & df_producers['tconst'].isin(valid_tconst_set)]
df_actors = df_actors[df_actors['nconst'].isin(valid_nconst_set) & df_actors['tconst'].isin(valid_tconst_set)]

print("Directors Data After validating FK: ",len(df_directors))
print("Writers Data After validating FK: ",len(df_writer))
print("Producers Data After validating FK: ",len(df_producers))
print("Actors Data After validating FK: ",len(df_actors))

df_directors.to_csv("cleaned_directors.tsv", sep='\t', index=False, encoding='utf-8')
df_writer.to_csv("cleaned_writers.tsv", sep='\t', index=False, encoding='utf-8')
df_producers.to_csv("cleaned_producers.tsv", sep='\t', index=False, encoding='utf-8')
df_actors.to_csv("cleaned_actors.tsv", sep='\t', index=False, encoding='utf-8')