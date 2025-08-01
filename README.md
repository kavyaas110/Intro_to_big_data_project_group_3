Following 3 files setup the relational database (Phase 1)  
1) Preprocessing_all_data_for_postgres.py: This file processes all the tsv file, cleans the data and makes clean csv file which could be used to load data into postgres database.  
2) SQL_TABLE_CREATION_COMMANDS.txt: This file contains all the SQL commands to create our database  
3) SQL_COPYING_DATA_COMMANDS.txt: This file contains all the commands to time and copy the data from csv files to the postgres database tables.  
  
Following 2 files setup MongoDB (Phase 2)  
4) MONGODB_COLLECTION_CREATION_COMMANDS.txt: This file gives command to setup MongoDB Collections  
5) insert_data_into_mongo_from_postgres.py: This file loads the data from Postgres to MongoDB.  
